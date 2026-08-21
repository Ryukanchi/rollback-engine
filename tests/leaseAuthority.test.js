const test = require("node:test");
const { describe } = require("node:test");
const assert = require("node:assert/strict");
const { join } = require("node:path");
const { tmpdir } = require("node:os");
const { randomUUID } = require("node:crypto");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const {
  createSqliteDatabase,
} = require("../src/infrastructure/sqlite/sqliteDatabase");
const {
  SqliteCommandStore,
} = require("../src/infrastructure/sqlite/sqliteCommandStore");
const {
  SqliteEventStore,
} = require("../src/infrastructure/sqlite/sqliteEventStore");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { COMMAND_STATUSES } = require("../src/application/storeContracts");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");
const {
  commandReceiptMetadata,
} = require("./support/commandReceiptFixtures");

const TTL = 5000;
const LONG_STEP = 12000;
const PAYLOAD = { item: "Widget", quantity: 1, amount: 100 };
const REVOKE_ERROR = {
  code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
  message: "Committed command events were found without a completed command result.",
  eventCommitted: true,
  retrySafe: false,
  retryAction: "MANUAL_RESOLUTION_REQUIRED",
};

function createEvent({ commandId, aggregateId = 1, sequence = 1, eventId }) {
  return createDomainEvent({
    eventId: eventId || `evt-${randomUUID()}`,
    eventType:
      sequence === 1 ? EVENT_TYPES.ORDER_CREATED : EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId,
    sequence,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload:
      sequence === 1
        ? { item: PAYLOAD.item, quantity: PAYLOAD.quantity }
        : { reservationId: 1, item: PAYLOAD.item, quantity: PAYLOAD.quantity },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });
}

/**
 * Lease time belongs to the command store, so a test cannot hand a chosen `now`
 * to a single mutation. Every store built here carries a mutable clock; a test
 * positions that clock at the moment it means and the store decides from there.
 */
const leaseClocks = new WeakMap();

function trackLeaseClock(store, clock) {
  leaseClocks.set(store, clock);
  return store;
}

/** Positions `store`'s lease clock at `ms`, then hands the store back. */
function at(store, ms) {
  const clock = leaseClocks.get(store);
  if (!clock) {
    throw new Error("this store has no test-owned lease clock to position");
  }
  clock.ms = ms;
  return store;
}

function createAdapters(type) {
  const leaseClock = { ms: 1000 };
  const leaseNow = () => leaseClock.ms;
  const adapters =
    type === "sqlite"
      ? createStorageAdapters({
          type: "sqlite",
          dbPath: join(tmpdir(), `rollback-lease-${randomUUID()}.db`),
          leaseNow,
        })
      : createStorageAdapters({ type: "memory", leaseNow });
  trackLeaseClock(adapters.commandStore, leaseClock);
  adapters.leaseClock = leaseClock;
  return adapters;
}

/** A directly wired SQLite store plus the lease clock the test drives it with. */
function sqliteStoreWithClock(db, startMs) {
  const clock = { ms: startMs };
  return trackLeaseClock(new SqliteCommandStore({ db, now: () => clock.ms }), clock);
}

function reserve(
  store,
  commandId,
  { workerId = "worker-A", leaseTtlMs = TTL, now = 1000, payload = { ...PAYLOAD } } = {}
) {
  return at(store, now).reserve({
    commandId,
    commandType: "CHECKOUT",
    payload,
    workerId,
    leaseTtlMs,
  });
}

/** The payload shape the engine itself normalises a checkout command into. */
const ENGINE_PAYLOAD = { ...PAYLOAD, simulateFailureAt: null };

/** Reserves a command and makes one authoritative event durable for it. */
function seedPartialCommit(
  adapters,
  commandId,
  { now = 1000, workerId = "worker-A", payload = { ...PAYLOAD } } = {}
) {
  reserve(adapters.commandStore, commandId, { workerId, now, payload });
  const event = createEvent({ commandId, sequence: 1 });
  adapters.eventStore.append(event, { expectedVersion: 0, fencingToken: 1 });
  adapters.commandStore.recordEvent(commandId, event, { fencingToken: 1 });
  return event;
}

// ===========================================================================
// Healthy slow worker: an uncontested generation survives its nominal TTL.
// ===========================================================================
for (const storeType of ["memory", "sqlite"]) {
  describe(`Healthy slow worker (${storeType})`, () => {
    /**
     * Runs a full checkout while burning more than the whole TTL at a chosen
     * checkpoint, and records the command row at every append so the test can
     * prove the generation was never touched during the slow interval.
     */
    function runSlowCheckout({ slowAt, command = PAYLOAD, commandId }) {
      const adapters = createAdapters(storeType);
      const leaseClock = adapters.leaseClock;
      leaseClock.ms = 1_000_000;
      const now = () => leaseClock.ms;

      const observations = [];
      let appends = 0;
      const realAppend = adapters.eventStore.append.bind(adapters.eventStore);
      adapters.eventStore.append = (event, options) => {
        appends += 1;
        if (slowAt === `before-event-${appends}`) {
          leaseClock.ms += LONG_STEP;
        }
        observations.push({
          at: `append-${appends}`,
          expired: adapters.commandStore.get(commandId).leaseExpiresAt <= now(),
          row: adapters.commandStore.get(commandId),
        });
        return realAppend(event, options);
      };

      const takeovers = [];
      const revocations = [];
      const realTakeover = adapters.commandStore.takeOverExpired.bind(adapters.commandStore);
      adapters.commandStore.takeOverExpired = (args) => {
        const outcome = realTakeover(args);
        takeovers.push(outcome);
        return outcome;
      };
      const realRevoke = adapters.commandStore.revokeExpired.bind(adapters.commandStore);
      adapters.commandStore.revokeExpired = (args) => {
        const outcome = realRevoke(args);
        revocations.push(outcome);
        return outcome;
      };

      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-A",
        leaseTtlMs: TTL,
        clock: () => new Date(now()).toISOString(),
      });

      const result = engine.checkout(command, { commandId });
      return { adapters, result, observations, takeovers, revocations, commandId };
    }

    function assertUncontestedThroughout({ adapters, observations, takeovers, revocations, commandId }) {
      assert.equal(takeovers.length, 0, "no takeover may occur");
      assert.equal(revocations.length, 0, "no revocation may occur");
      assert.ok(
        observations.some((o) => o.expired),
        "the test must actually cross the expiry boundary, otherwise it proves nothing"
      );
      for (const observation of observations) {
        assert.equal(observation.row.status, COMMAND_STATUSES.PROCESSING, `${observation.at}: status`);
        assert.equal(observation.row.leaseToken, 1, `${observation.at}: generation must not move`);
        assert.equal(observation.row.leaseOwner, "worker-A", `${observation.at}: owner must not move`);
      }
      const final = adapters.commandStore.get(commandId);
      assert.equal(final.status, COMMAND_STATUSES.COMPLETED);
      assert.equal(final.leaseToken, 1);
    }

    test("HSL-1: a slow step before the first event does not self-fence", () => {
      const run = runSlowCheckout({ slowAt: "before-event-1", commandId: "hsl-1" });
      assert.equal(run.result.status, "completed");
      assert.equal(run.adapters.eventStore.getByCommandId("hsl-1").length, 3);
      assertUncontestedThroughout(run);
      run.adapters.close();
    });

    test("HSL-2: a slow step between committed events does not self-fence", () => {
      const run = runSlowCheckout({ slowAt: "before-event-2", commandId: "hsl-2" });
      assert.equal(run.result.status, "completed");
      assert.equal(run.adapters.eventStore.getByCommandId("hsl-2").length, 3);
      assertUncontestedThroughout(run);
      run.adapters.close();
    });

    test("HSL-3: a slow compensation step does not self-fence", () => {
      const run = runSlowCheckout({
        slowAt: "before-event-5",
        command: { ...PAYLOAD, simulateFailureAt: "after_payment" },
        commandId: "hsl-3",
      });
      assert.equal(run.result.status, "rolled_back");
      assert.deepEqual(
        run.result.events.map((event) => event.eventType),
        [
          EVENT_TYPES.ORDER_CREATED,
          EVENT_TYPES.INVENTORY_RESERVED,
          EVENT_TYPES.PAYMENT_CHARGED,
          EVENT_TYPES.PAYMENT_REFUNDED,
          EVENT_TYPES.INVENTORY_RELEASED,
          EVENT_TYPES.ORDER_ROLLED_BACK,
        ],
        "compensation must run to completion, in order"
      );
      assertUncontestedThroughout(run);
      run.adapters.close();
    });
  });
}

// ===========================================================================
// revokeExpired: eligibility, atomicity and the races against owner mutations.
// ===========================================================================
for (const storeType of ["memory", "sqlite"]) {
  describe(`Authority revocation (${storeType})`, () => {
    test("revokes a partially committed expired generation in one transition", () => {
      const adapters = createAdapters(storeType);
      const event = seedPartialCommit(adapters, "rev-ok");

      const outcome = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-ok",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });

      assert.equal(outcome.success, true);
      const row = adapters.commandStore.get("rev-ok");
      assert.equal(row.status, COMMAND_STATUSES.FAILED);
      assert.equal(row.leaseToken, 1, "revocation must not advance the generation");
      assert.equal(row.leaseOwner, null);
      assert.equal(row.leaseExpiresAt, null);
      assert.equal(row.error.code, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");
      assert.equal(row.aggregateId, 1);
      assert.deepEqual(row.eventRange.eventIds, [event.eventId]);
      assert.deepEqual(row.error.eventIds, [event.eventId]);
      adapters.close();
    });

    test("CE-12: a stale expiry observation cannot revoke a renewed owner", () => {
      const adapters = createAdapters(storeType);
      seedPartialCommit(adapters, "rev-ce12");

      // A third party observes the lease as expired at t = 60_000 ...
      const observed = adapters.commandStore.get("rev-ce12");
      assert.ok(observed.leaseExpiresAt <= 60_000, "precondition: observed as expired");

      // ... but the healthy owner renews before the revocation is attempted.
      at(adapters.commandStore, 60_000).renewLease({
        commandId: "rev-ce12",
        workerId: "worker-A",
        fencingToken: 1,
        leaseTtlMs: TTL,
      });

      const outcome = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-ce12",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });

      assert.equal(outcome.success, false);
      assert.equal(outcome.reason, "NOT_EXPIRED", "expiry must be revalidated at mutation time");

      const row = adapters.commandStore.get("rev-ce12");
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING, "the healthy owner survives");
      assert.equal(row.leaseToken, 1);
      assert.equal(row.leaseOwner, "worker-A");
      assert.equal(row.error, null);

      // Control: without the renewal the very same call succeeds, so the
      // rejection above was the expiry revalidation and nothing else.
      const control = createAdapters(storeType);
      seedPartialCommit(control, "rev-ce12");
      const controlOutcome = at(control.commandStore, 60_000).revokeExpired({
        commandId: "rev-ce12",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      assert.equal(controlOutcome.success, true);
      control.close();
      adapters.close();
    });

    test("LA-14: eligibility comes from the event history, not from eventRange", () => {
      const adapters = createAdapters(storeType);
      reserve(adapters.commandStore, "rev-hist");

      // Bookkeeping says "no events". The event history says otherwise.
      const event = createEvent({ commandId: "rev-hist", sequence: 1 });
      adapters.eventStore.append(event, { expectedVersion: 0, fencingToken: 1 });
      assert.equal(adapters.commandStore.get("rev-hist").eventRange, null);

      const outcome = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-hist",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });

      assert.equal(outcome.success, true, "a lagging eventRange must not hide a committed event");
      const row = adapters.commandStore.get("rev-hist");
      assert.deepEqual(row.eventRange.eventIds, [event.eventId]);
      adapters.close();
    });

    test("Phase 8: revocation persists the FULL authoritative range in one step", () => {
      const adapters = createAdapters(storeType);
      const first = seedPartialCommit(adapters, "rev-atomic");

      // A second event is durable but was never recorded in the bookkeeping.
      const second = createEvent({ commandId: "rev-atomic", sequence: 2 });
      adapters.eventStore.append(second, { expectedVersion: 1, fencingToken: 1 });
      assert.equal(adapters.commandStore.get("rev-atomic").eventRange.eventIds.length, 1);
      assert.equal(adapters.eventStore.getByCommandId("rev-atomic").length, 2);

      const outcome = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-atomic",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });

      assert.equal(outcome.success, true);
      const row = adapters.commandStore.get("rev-atomic");
      assert.equal(row.status, COMMAND_STATUSES.FAILED);
      assert.deepEqual(
        row.eventRange.eventIds,
        [first.eventId, second.eventId],
        "the terminal row must never be durable with a stale range"
      );
      assert.equal(row.eventRange.lastSequence, 2);
      assert.deepEqual(row.error.eventIds, [first.eventId, second.eventId]);
      adapters.close();
    });

    test("refuses a zero-event command so takeover stays the only route", () => {
      const adapters = createAdapters(storeType);
      reserve(adapters.commandStore, "rev-zero");

      const outcome = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-zero",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });

      assert.equal(outcome.success, false);
      assert.equal(outcome.reason, "NO_EVENTS");
      assert.equal(adapters.commandStore.get("rev-zero").status, COMMAND_STATUSES.PROCESSING);
      adapters.close();
    });

    test("refuses a stale generation", () => {
      const adapters = createAdapters(storeType);
      seedPartialCommit(adapters, "rev-gen");

      const outcome = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-gen",
        expectedToken: 99,
        error: REVOKE_ERROR,
      });

      assert.equal(outcome.success, false);
      assert.equal(outcome.reason, "TOKEN_MISMATCH");
      assert.equal(adapters.commandStore.get("rev-gen").status, COMMAND_STATUSES.PROCESSING);
      adapters.close();
    });

    // --- RACE-3..8 -------------------------------------------------------

    test("RACE-3/4: append vs revoke has one winner in each direction", () => {
      // Append first: the revocation reads the newly committed event.
      const first = createAdapters(storeType);
      const seeded = seedPartialCommit(first, "race-append");
      const late = createEvent({ commandId: "race-append", sequence: 2 });
      first.eventStore.append(late, { expectedVersion: 1, fencingToken: 1 });
      const afterAppend = at(first.commandStore, 60_000).revokeExpired({
        commandId: "race-append",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      assert.equal(afterAppend.success, true);
      assert.deepEqual(
        first.commandStore.get("race-append").eventRange.eventIds,
        [seeded.eventId, late.eventId],
        "a revocation that lost the race still records what actually happened"
      );
      first.close();

      // Revoke first: the owner's append is rejected.
      const second = createAdapters(storeType);
      seedPartialCommit(second, "race-append");
      at(second.commandStore, 60_000).revokeExpired({
        commandId: "race-append",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      assert.throws(
        () =>
          second.eventStore.append(createEvent({ commandId: "race-append", sequence: 2 }), {
            expectedVersion: 1,
            fencingToken: 1,
          }),
        (error) => {
          assert.equal(error.code, "FENCING_TOKEN_STALE");
          return true;
        }
      );
      assert.equal(second.eventStore.getByCommandId("race-append").length, 1);
      second.close();
    });

    test("RACE-5/6: complete vs revoke yields exactly one terminal truth", () => {
      const completeFirst = createAdapters(storeType);
      seedPartialCommit(completeFirst, "race-complete");
      completeFirst.commandStore.complete("race-complete", { ok: true }, {
        fencingToken: 1,
        receiptMetadata: commandReceiptMetadata({ domainEffect: "events" }),
      });
      const revokeAfter = at(completeFirst.commandStore, 60_000).revokeExpired({
        commandId: "race-complete",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      assert.equal(revokeAfter.success, false);
      assert.equal(revokeAfter.reason, "NOT_PROCESSING");
      assert.equal(
        completeFirst.commandStore.get("race-complete").status,
        COMMAND_STATUSES.COMPLETED
      );
      completeFirst.close();

      const revokeFirst = createAdapters(storeType);
      seedPartialCommit(revokeFirst, "race-complete");
      assert.equal(
        at(revokeFirst.commandStore, 60_000).revokeExpired({
          commandId: "race-complete",
          expectedToken: 1,
          error: REVOKE_ERROR,
        }).success,
        true
      );
      assert.throws(
        () => revokeFirst.commandStore.complete("race-complete", { ok: true }, { fencingToken: 1 }),
        /is not processing/
      );
      assert.equal(
        revokeFirst.commandStore.get("race-complete").status,
        COMMAND_STATUSES.FAILED
      );
      revokeFirst.close();
    });

    test("RACE-7/8: renew vs revoke has one winner in each direction", () => {
      // Renew first -> revocation is refused as not expired (this is CE-12).
      const renewFirst = createAdapters(storeType);
      seedPartialCommit(renewFirst, "race-renew");
      at(renewFirst.commandStore, 60_000).renewLease({
        commandId: "race-renew",
        workerId: "worker-A",
        fencingToken: 1,
        leaseTtlMs: TTL,
      });
      assert.equal(
        at(renewFirst.commandStore, 60_000).revokeExpired({
          commandId: "race-renew",
          expectedToken: 1,
          error: REVOKE_ERROR,
        }).reason,
        "NOT_EXPIRED"
      );
      assert.equal(
        renewFirst.commandStore.get("race-renew").status,
        COMMAND_STATUSES.PROCESSING
      );
      renewFirst.close();

      // Revoke first -> the owner's renewal is refused because it is terminal.
      const revokeFirst = createAdapters(storeType);
      seedPartialCommit(revokeFirst, "race-renew");
      assert.equal(
        at(revokeFirst.commandStore, 60_000).revokeExpired({
          commandId: "race-renew",
          expectedToken: 1,
          error: REVOKE_ERROR,
        }).success,
        true
      );
      assert.throws(
        () =>
          at(revokeFirst.commandStore, 60_000).renewLease({
            commandId: "race-renew",
            workerId: "worker-A",
            fencingToken: 1,
            leaseTtlMs: TTL,
          }),
        /is not processing/
      );
      revokeFirst.close();
    });

    test("Phase 9: a repeated revocation is idempotent and mutates nothing", () => {
      const adapters = createAdapters(storeType);
      const event = seedPartialCommit(adapters, "rev-ack");

      const first = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-ack",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      assert.equal(first.success, true);
      const afterFirst = adapters.commandStore.get("rev-ack");

      const second = at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "rev-ack",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      assert.equal(second.success, false);
      assert.equal(second.reason, "NOT_PROCESSING");

      assert.deepEqual(adapters.commandStore.get("rev-ack"), afterFirst, "no second transition");
      assert.equal(adapters.commandStore.get("rev-ack").leaseToken, 1, "no new generation");
      assert.equal(adapters.eventStore.getByCommandId("rev-ack").length, 1, "history untouched");
      assert.deepEqual(afterFirst.eventRange.eventIds, [event.eventId]);
      adapters.close();
    });

    // --- Suspend / resume -------------------------------------------------

    test("SUSPEND-1: an expired uncontested owner may still renew, append and complete", () => {
      const adapters = createAdapters(storeType);
      seedPartialCommit(adapters, "suspend-1");

      assert.equal(
        at(adapters.commandStore, 60_000).renewLease({
          commandId: "suspend-1",
          workerId: "worker-A",
          fencingToken: 1,
          leaseTtlMs: TTL,
        }).renewed,
        true
      );
      const appended = adapters.eventStore.append(
        createEvent({ commandId: "suspend-1", sequence: 2 }),
        { expectedVersion: 1, fencingToken: 1 }
      );
      assert.equal(appended.sequence, 2);
      assert.equal(
        adapters.commandStore.complete("suspend-1", { ok: true }, {
          fencingToken: 1,
          receiptMetadata: commandReceiptMetadata({ domainEffect: "events" }),
        }).status,
        COMMAND_STATUSES.COMPLETED
      );
      adapters.close();
    });

    test("SUSPEND-2: after a zero-event takeover the old generation is rejected", () => {
      const adapters = createAdapters(storeType);
      reserve(adapters.commandStore, "suspend-2");
      const takeover = at(adapters.commandStore, 60_000).takeOverExpired({
        commandId: "suspend-2",
        workerId: "worker-B",
        leaseTtlMs: TTL,
        expectedToken: 1,
      });
      assert.equal(takeover.success, true);
      assert.equal(takeover.record.leaseToken, 2);

      assert.throws(
        () =>
          at(adapters.commandStore, 60_000).renewLease({
            commandId: "suspend-2",
            workerId: "worker-A",
            fencingToken: 1,
            leaseTtlMs: TTL,
          }),
        (error) => error.code === "FENCING_TOKEN_STALE"
      );
      assert.throws(
        () =>
          adapters.eventStore.append(createEvent({ commandId: "suspend-2", sequence: 1 }), {
            expectedVersion: 0,
            fencingToken: 1,
          }),
        (error) => error.code === "FENCING_TOKEN_STALE"
      );
      assert.throws(
        () => adapters.commandStore.complete("suspend-2", {}, { fencingToken: 1 }),
        (error) => error.code === "FENCING_TOKEN_STALE"
      );
      adapters.close();
    });

    test("SUSPEND-3: after revocation the old execution path is rejected by status", () => {
      const adapters = createAdapters(storeType);
      seedPartialCommit(adapters, "suspend-3");
      at(adapters.commandStore, 60_000).revokeExpired({
        commandId: "suspend-3",
        expectedToken: 1,
        error: REVOKE_ERROR,
      });

      assert.throws(
        () =>
          at(adapters.commandStore, 60_000).renewLease({
            commandId: "suspend-3",
            workerId: "worker-A",
            fencingToken: 1,
            leaseTtlMs: TTL,
          }),
        /is not processing/
      );
      assert.throws(
        () =>
          adapters.eventStore.append(createEvent({ commandId: "suspend-3", sequence: 2 }), {
            expectedVersion: 1,
            fencingToken: 1,
          }),
        (error) => error.code === "FENCING_TOKEN_STALE"
      );
      assert.throws(
        () => adapters.commandStore.complete("suspend-3", {}, { fencingToken: 1 }),
        /is not processing/
      );
      adapters.close();
    });

    test("OWNER-1: the same owner across generations is still fenced", () => {
      const adapters = createAdapters(storeType);
      reserve(adapters.commandStore, "owner-1", { workerId: "worker-A" });
      const takeover = at(adapters.commandStore, 60_000).takeOverExpired({
        commandId: "owner-1",
        workerId: "worker-A", // same identity, new generation
        leaseTtlMs: TTL,
        expectedToken: 1,
      });
      assert.equal(takeover.record.leaseToken, 2);
      assert.equal(takeover.record.leaseOwner, "worker-A");

      assert.throws(
        () =>
          at(adapters.commandStore, 60_000).renewLease({
            commandId: "owner-1",
            workerId: "worker-A",
            fencingToken: 1,
            leaseTtlMs: TTL,
          }),
        (error) => {
          assert.equal(error.code, "FENCING_TOKEN_STALE");
          assert.equal(error.providedToken, 1);
          assert.equal(error.currentToken, 2);
          return true;
        }
      );
      // Control: the identical call from the live generation succeeds.
      assert.equal(
        at(adapters.commandStore, 60_000).renewLease({
          commandId: "owner-1",
          workerId: "worker-A",
          fencingToken: 2,
          leaseTtlMs: TTL,
        }).renewed,
        true
      );
      adapters.close();
    });
  });
}

// ===========================================================================
// Coordinator level: post-first-event death and zero-event boundaries.
// ===========================================================================
for (const storeType of ["memory", "sqlite"]) {
  describe(`Lease authority end to end (${storeType})`, () => {
    function engineOn(adapters, workerId, now) {
      return new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId,
        leaseTtlMs: TTL,
        clock: () => new Date(now()).toISOString(),
      });
    }

    test("post-first-event death: retry revokes once and then stays stable", () => {
      const adapters = createAdapters(storeType);
      // A real epoch base keeps engine-generated timestamps after the seeded one.
      const base = Date.parse("2026-08-15T12:00:00.000Z");
      const leaseClock = adapters.leaseClock;
      leaseClock.ms = base;
      const clockOf = () => leaseClock.ms;
      const event = seedPartialCommit(adapters, "death-1", {
        now: base,
        payload: ENGINE_PAYLOAD,
      });

      // The owner is gone. Its lease is still valid, so a retry must wait.
      leaseClock.ms = base + 1000;
      const engine = engineOn(adapters, "worker-B", clockOf);
      assert.throws(
        () => engine.checkout(PAYLOAD, { commandId: "death-1" }),
        (error) => error.code === "COMMAND_IN_PROGRESS"
      );

      leaseClock.ms = base + 90_000;
      const outcomes = [];
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          engine.checkout(PAYLOAD, { commandId: "death-1" });
          outcomes.push("SUCCEEDED");
        } catch (error) {
          outcomes.push(error.code);
        }
      }
      assert.deepEqual(outcomes, [
        "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
        "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
        "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
      ]);

      const row = adapters.commandStore.get("death-1");
      assert.equal(row.status, COMMAND_STATUSES.FAILED);
      assert.equal(row.leaseToken, 1, "revocation keeps the generation");
      assert.equal(row.error.code, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");
      assert.deepEqual(row.eventRange.eventIds, [event.eventId]);
      assert.equal(adapters.eventStore.getByCommandId("death-1").length, 1, "no duplicates");

      // Takeover of a partially committed command stays forbidden.
      assert.equal(
        at(adapters.commandStore, leaseClock.ms).takeOverExpired({
          commandId: "death-1",
          workerId: "worker-C",
          leaseTtlMs: TTL,
        }).success,
        false
      );

      // Explicit compensation through a NEW command remains possible.
      const compensation = engine.compensate(1, "operator cleanup", { commandId: "death-1-fix" });
      assert.equal(compensation.status, "rolled_back");
      adapters.close();
    });

    test("ZERO-2: a zero-event command completes after expiry when uncontested", () => {
      const adapters = createAdapters(storeType);
      const leaseClock = adapters.leaseClock;
      leaseClock.ms = 1_000_000;
      const clockOf = () => leaseClock.ms;
      const engine = engineOn(adapters, "worker-A", clockOf);

      engine.checkout({ ...PAYLOAD, simulateFailureAt: "after_payment" }, { commandId: "zero-setup" });
      assert.equal(engine.replay(1).lifecycle, "rolled_back");

      leaseClock.ms += 10 * LONG_STEP;
      const compensation = engine.compensate(1, "already compensated", { commandId: "zero-2" });
      assert.equal(compensation.events.length, 0, "this path never reaches commitEvent()");
      assert.equal(adapters.commandStore.get("zero-2").status, COMMAND_STATUSES.COMPLETED);
      adapters.close();
    });

    test("ZERO-1/3: an expired zero-event command is taken over, old generation rejected", () => {
      const adapters = createAdapters(storeType);
      reserve(adapters.commandStore, "zero-1");
      const takeover = at(adapters.commandStore, 60_000).takeOverExpired({
        commandId: "zero-1",
        workerId: "worker-B",
        leaseTtlMs: TTL,
        expectedToken: 1,
      });
      assert.equal(takeover.success, true);
      assert.equal(takeover.record.leaseToken, 2);
      assert.throws(
        () => adapters.commandStore.complete("zero-1", {}, { fencingToken: 1 }),
        (error) => error.code === "FENCING_TOKEN_STALE"
      );
      adapters.close();
    });
  });
}

// ===========================================================================
// Real multi-connection SQLite interleavings.
// ===========================================================================
describe("Lease authority under SQLite concurrency", () => {
  /** Fires a barrier immediately after a command SELECT resolves. */
  function withBarrierAfterSelect(db, onAfterSelect) {
    return {
      prepare(sql) {
        const statement = db.prepare(sql);
        const isCommandSelect = /SELECT[\s\S]*FROM commands[\s\S]*WHERE command_id/i.test(sql);
        return {
          get(...args) {
            const row = statement.get(...args);
            if (isCommandSelect) onAfterSelect();
            return row;
          },
          all: (...args) => statement.all(...args),
          run: (...args) => statement.run(...args),
        };
      },
      exec: (sql) => db.exec(sql),
      close: () => db.close(),
    };
  }

  test("SQLITE-0: no connection ever observes a terminal row with a stale range", () => {
    const dbPath = join(tmpdir(), `rollback-lease-atomic-${randomUUID()}.db`);
    const dbA = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
    const dbB = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
    const observer = new SqliteCommandStore({ db: dbB, now: () => 90_000 });
    const commandId = "sqlite-revoke-atomicity";

    // Every write connection A performs during the revocation is sampled from a
    // second connection. A split revoke/reconcile design would expose a durable
    // "failed with a lagging range" row to that observer; an atomic one cannot.
    const samples = [];
    let sampling = false;
    function samplingDb(db) {
      return {
        prepare(sql) {
          const statement = db.prepare(sql);
          const isWrite = /^\s*(UPDATE|INSERT|DELETE)/i.test(sql);
          return {
            get: (...args) => statement.get(...args),
            all: (...args) => statement.all(...args),
            run(...args) {
              const result = statement.run(...args);
              if (sampling && isWrite) samples.push(observer.get(commandId));
              return result;
            },
          };
        },
        exec: (sql) => db.exec(sql),
        close: () => db.close(),
      };
    }

    const storeA = sqliteStoreWithClock(samplingDb(dbA), 1000);
    const eventA = new SqliteEventStore({ db: dbA });

    try {
      at(storeA, 1000).reserve({
        commandId,
        commandType: "CHECKOUT",
        payload: { ...PAYLOAD },
        workerId: "worker-A",
        leaseTtlMs: TTL,
      });
      const first = createEvent({ commandId, sequence: 1 });
      eventA.append(first, { expectedVersion: 0, fencingToken: 1 });
      storeA.recordEvent(commandId, first, { fencingToken: 1 });

      // A second event is durable but deliberately not recorded, so a split
      // design would have a genuinely stale range to expose.
      const second = createEvent({ commandId, sequence: 2 });
      eventA.append(second, { expectedVersion: 1, fencingToken: 1 });
      const authoritative = [first.eventId, second.eventId];

      sampling = true;
      const outcome = at(storeA, 90_000).revokeExpired({
        commandId,
        expectedToken: 1,
        error: REVOKE_ERROR,
      });
      sampling = false;

      assert.equal(outcome.success, true);
      assert.ok(samples.length > 0, "the revocation must have been sampled mid-flight");

      for (const sample of samples) {
        const staleTerminal =
          sample.status === COMMAND_STATUSES.FAILED &&
          (sample.eventRange === null ||
            sample.eventRange.eventIds.length !== authoritative.length);
        assert.equal(
          staleTerminal,
          false,
          `observer saw a terminal row with a stale range: ${JSON.stringify(sample)}`
        );
      }

      const final = observer.get(commandId);
      assert.equal(final.status, COMMAND_STATUSES.FAILED);
      assert.deepEqual(final.eventRange.eventIds, authoritative);
    } finally {
      dbA.close();
      dbB.close();
    }
  });

  test("SQLITE-0b: a generation change inside the revocation window matches no row", () => {
    const dbPath = join(tmpdir(), `rollback-lease-cas-${randomUUID()}.db`);
    const db = createSqliteDatabase({ path: dbPath });
    const commandId = "sqlite-revoke-cas";

    let armed = false;
    const casClock = { ms: 1000 };
    const store = trackLeaseClock(
      new SqliteCommandStore({
        db: {
        prepare(sql) {
          const statement = db.prepare(sql);
          const isCommandSelect = /SELECT[\s\S]*FROM commands[\s\S]*WHERE command_id/i.test(sql);
          return {
            get(...args) {
              const row = statement.get(...args);
              if (isCommandSelect && armed) {
                armed = false;
                // The generation moves after revokeExpired validated it but
                // before it writes.
                db.prepare(
                  "UPDATE commands SET lease_token = lease_token + 1 WHERE command_id = ?"
                ).run(commandId);
              }
              return row;
            },
            all: (...args) => statement.all(...args),
            run: (...args) => statement.run(...args),
          };
        },
        exec: (sql) => db.exec(sql),
          close: () => db.close(),
        },
        now: () => casClock.ms,
      }),
      casClock
    );
    const eventStore = new SqliteEventStore({ db });

    try {
      at(store, 1000).reserve({
        commandId,
        commandType: "CHECKOUT",
        payload: { ...PAYLOAD },
        workerId: "worker-A",
        leaseTtlMs: TTL,
      });
      const event = createEvent({ commandId, sequence: 1 });
      eventStore.append(event, { expectedVersion: 0, fencingToken: 1 });
      store.recordEvent(commandId, event, { fencingToken: 1 });

      armed = true;
      assert.throws(
        () =>
          at(store, 90_000).revokeExpired({
            commandId,
            expectedToken: 1,
            error: REVOKE_ERROR,
          }),
        (error) => {
          assert.equal(error.code, "FENCING_TOKEN_STALE");
          return true;
        }
      );

      // Nothing the stale revoker attempted survived.
      const row = store.get(commandId);
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(row.error, null);
    } finally {
      db.close();
    }
  });

  test("SQLITE-1: revoke cannot commit between append's validation and its insert", () => {
    const dbPath = join(tmpdir(), `rollback-lease-append-${randomUUID()}.db`);
    const dbA = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
    const dbB = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
    const storeB = new SqliteCommandStore({ db: dbB, now: () => 90_000 });
    const commandId = "sqlite-append-vs-revoke";

    let armed = false;
    let revocation = null;
    let revokeAttempts = 0;
    const commandA = sqliteStoreWithClock(dbA, 1000);

    // storeB already reads a fixed lease clock of 90_000, so the revocation it
    // attempts is temporally eligible and only SQLite's write lock can stop it.
    // The counter proves the attempt actually entered the store: a revocation
    // that died in test setup would never reach this far, and the race the test
    // is named for would silently not happen.
    const realRevoke = storeB.revokeExpired.bind(storeB);
    storeB.revokeExpired = (args) => {
      revokeAttempts += 1;
      return realRevoke(args);
    };

    const eventA = new SqliteEventStore({
      db: withBarrierAfterSelect(dbA, () => {
        if (!armed) return;
        armed = false;
        try {
          revocation = storeB.revokeExpired({
            commandId,
            expectedToken: 1,
            error: REVOKE_ERROR,
          });
        } catch (error) {
          revocation = { success: false, reason: "BLOCKED", error };
        }
      }),
    });

    try {
      at(commandA, 1000).reserve({
        commandId,
        commandType: "CHECKOUT",
        payload: { ...PAYLOAD },
        workerId: "worker-A",
        leaseTtlMs: TTL,
      });
      const first = createEvent({ commandId, sequence: 1 });
      eventA.append(first, { expectedVersion: 0, fencingToken: 1 });
      commandA.recordEvent(commandId, first, { fencingToken: 1 });

      armed = true;
      let appendError = null;
      try {
        eventA.append(createEvent({ commandId, sequence: 2 }), {
          expectedVersion: 1,
          fencingToken: 1,
        });
      } catch (error) {
        appendError = error;
      }

      assert.notEqual(revocation, null, "the barrier must have run inside the append window");
      assert.equal(
        revokeAttempts,
        1,
        "the competing revocation must actually reach SqliteCommandStore.revokeExpired"
      );
      assert.notEqual(
        revocation.success,
        true,
        "a revocation must never commit inside an append transaction"
      );

      // The refusal has to come from SQLite's write lock. Anything else - a
      // helper that throws, an argument rejection, a missing precondition -
      // would mean the revocation never contended with the append at all, and
      // the test would be passing without exercising its own subject.
      assert.equal(revocation.reason, "BLOCKED");
      assert.equal(
        revocation.error.code,
        "ERR_SQLITE_ERROR",
        `the block must come from SQLite, got: ${revocation.error.message}`
      );
      assert.equal(
        revocation.error.errcode,
        5,
        "SQLITE_BUSY: the append transaction still held the write lock"
      );

      // Whichever way contention resolved, the row is never terminal while its
      // range disagrees with the authoritative history.
      const row = storeB.get(commandId);
      const events = eventA.getByCommandId(commandId);
      if (row.status === COMMAND_STATUSES.FAILED) {
        assert.deepEqual(
          row.eventRange.eventIds,
          events.map((event) => event.eventId)
        );
      } else {
        assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
        assert.equal(appendError, null, "an uncontested append must succeed");
        assert.equal(events.length, 2);
      }
    } finally {
      dbA.close();
      dbB.close();
    }
  });

  for (const [label, ownerMutation] of [
    [
      "SQLITE-2: complete",
      (store, commandId) =>
        store.complete(commandId, { ok: true }, {
          fencingToken: 1,
          receiptMetadata: commandReceiptMetadata({ domainEffect: "events" }),
        }),
    ],
    [
      "SQLITE-3: renew",
      (store, commandId) =>
        at(store, 90_000).renewLease({
          commandId,
          workerId: "worker-A",
          fencingToken: 1,
          leaseTtlMs: TTL,
        }),
    ],
  ]) {
    test(`${label} vs revoke across two connections has one winner`, () => {
      const dbPath = join(tmpdir(), `rollback-lease-owner-${randomUUID()}.db`);
      const dbA = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
      const dbB = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
      const storeB = new SqliteCommandStore({ db: dbB, now: () => 90_000 });
      const commandId = "sqlite-owner-vs-revoke";

      let armed = false;
      let revocation = null;
      const storeA = sqliteStoreWithClock(
        withBarrierAfterSelect(dbA, () => {
          if (!armed) return;
          armed = false;
          try {
            revocation = storeB.revokeExpired({
              commandId,
              expectedToken: 1,
              error: REVOKE_ERROR,
            });
          } catch (error) {
            revocation = { success: false, reason: "BLOCKED", error };
          }
        }),
        1000
      );
      const eventA = new SqliteEventStore({ db: dbA });

      try {
        at(storeA, 1000).reserve({
          commandId,
          commandType: "CHECKOUT",
          payload: { ...PAYLOAD },
          workerId: "worker-A",
          leaseTtlMs: TTL,
        });
        const seeded = createEvent({ commandId, sequence: 1 });
        eventA.append(seeded, { expectedVersion: 0, fencingToken: 1 });
        storeA.recordEvent(commandId, seeded, { fencingToken: 1 });

        armed = true;
        let ownerError = null;
        try {
          ownerMutation(storeA, commandId);
        } catch (error) {
          ownerError = error;
        }

        assert.notEqual(revocation, null, "the barrier must have run inside the owner transaction");
        assert.notEqual(
          revocation.success,
          true,
          "a revocation must never commit inside an owner transaction"
        );

        const row = storeB.get(commandId);
        assert.notEqual(
          row.status === COMMAND_STATUSES.FAILED && ownerError === null,
          true,
          "the owner and the revoker must not both report success"
        );
      } finally {
        dbA.close();
        dbB.close();
      }
    });
  }
});


// ===========================================================================
// D-1: a stable NOT_EXPIRED answer to a zero-event takeover must terminate.
//
// The Coordinator picks *which* challenge is meaningful from persisted facts;
// the Store alone decides temporal eligibility, from its own clock. When the
// Store keeps answering the same stable NOT_EXPIRED, re-resolving the unchanged
// row cannot make progress, so the answer has to terminate as
// COMMAND_IN_PROGRESS - exactly as the has-events revocation path does.
// ===========================================================================
for (const storeType of ["memory", "sqlite"]) {
  describe(`Zero-event takeover termination (${storeType})`, () => {
    test("D-1: a stable NOT_EXPIRED terminates in one challenge, not a resolution loop", () => {
      const adapters = createAdapters(storeType);
      const commandId = "d1-stable-not-expired";

      // Lease is born at 1000 and runs to 6000.
      reserve(adapters.commandStore, commandId, {
        workerId: "worker-A",
        now: 1000,
        payload: ENGINE_PAYLOAD,
      });
      const before = adapters.commandStore.get(commandId);

      const challenges = [];
      const realTakeover = adapters.commandStore.takeOverExpired.bind(
        adapters.commandStore
      );
      adapters.commandStore.takeOverExpired = (args) => {
        const outcome = realTakeover(args);
        challenges.push(outcome);
        return outcome;
      };

      // The Store's clock still says the lease is live. The Coordinator has no
      // lease clock of its own to disagree with it, so it must challenge and
      // then act on the answer it gets.
      adapters.leaseClock.ms = 1500;
      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-B",
        leaseTtlMs: TTL,
        clock: () => new Date(adapters.leaseClock.ms).toISOString(),
      });

      assert.throws(
        () => engine.checkout(PAYLOAD, { commandId }),
        (error) => error.code === "COMMAND_IN_PROGRESS",
        "a stable NOT_EXPIRED must surface as COMMAND_IN_PROGRESS"
      );

      // Anti-vacuity: the Store really was challenged, and really refused.
      assert.equal(
        challenges.length,
        1,
        "the Store must be challenged exactly once, not re-challenged in a loop"
      );
      assert.deepEqual(challenges[0], { success: false, reason: "NOT_EXPIRED" });

      // A refused challenge mutates nothing.
      const after = adapters.commandStore.get(commandId);
      assert.deepEqual(after, before, "a refused challenge must not touch the row");
      assert.equal(after.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(after.leaseToken, 1, "no generation increment");
      assert.equal(after.leaseOwner, "worker-A", "the owner keeps its lease");
      assert.equal(
        adapters.eventStore.getByCommandId(commandId).length,
        0,
        "no event may be appended by a refused challenger"
      );

      adapters.close();
    });
  });
}
