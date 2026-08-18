const test = require("node:test");
const { describe } = require("node:test");
const assert = require("node:assert/strict");
const { join } = require("node:path");
const { tmpdir } = require("node:os");
const { randomUUID } = require("node:crypto");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const {
  CommandExecutionCoordinator,
} = require("../src/application/commandExecutionCoordinator");
const { COMMAND_STATUSES } = require("../src/application/storeContracts");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");

const TTL = 5000;
const BIRTH = 1000;
const EXPIRY = BIRTH + TTL; // 6000
const PAYLOAD = { item: "Widget", quantity: 1, amount: 100 };
/** The shape the engine itself normalises a checkout command into. */
const ENGINE_PAYLOAD = { ...PAYLOAD, simulateFailureAt: null };
const REVOKE_ERROR = {
  code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
  message: "Committed command events were found without a completed command result.",
  eventCommitted: true,
  retrySafe: false,
  retryAction: "MANUAL_RESOLUTION_REQUIRED",
};

/** An absurd instant a mutation caller might try to smuggle in. */
const CALLER_FUTURE = 999_999_999;
/** An absurd instant a mutation caller might try to stall a challenge with. */
const CALLER_PAST = -999_999_999;

function makeAdapters(storeType, clock, dbPath) {
  return storeType === "sqlite"
    ? createStorageAdapters({
        type: "sqlite",
        dbPath: dbPath ?? join(tmpdir(), `rollback-temporal-${randomUUID()}.db`),
        leaseNow: () => clock.ms,
      })
    : createStorageAdapters({ type: "memory", leaseNow: () => clock.ms });
}

/** Adapters whose command store reads `clock`, plus the clock itself. */
function withClock(storeType, startMs = BIRTH) {
  const clock = { ms: startMs };
  return { clock, adapters: makeAdapters(storeType, clock) };
}

function reserveAt(adapters, clock, commandId, ms = BIRTH, payload = { ...PAYLOAD }) {
  clock.ms = ms;
  return adapters.commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload,
    workerId: "worker-A",
    leaseTtlMs: TTL,
  });
}

/** Reserves and makes one authoritative event durable, so only revocation applies. */
function seedPartialCommit(adapters, clock, commandId, ms = BIRTH) {
  reserveAt(adapters, clock, commandId, ms);
  const event = createDomainEvent({
    eventId: `evt-${randomUUID()}`,
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: PAYLOAD.item, quantity: PAYLOAD.quantity },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });
  adapters.eventStore.append(event, { expectedVersion: 0, fencingToken: 1 });
  adapters.commandStore.recordEvent(commandId, event, { fencingToken: 1 });
  return event;
}

// ===========================================================================
// The Command Store owns lease time. Both adapters are held to this
// identically: the mechanisms differ (BEGIN IMMEDIATE vs one uninterrupted
// synchronous operation), the contract does not.
// ===========================================================================
for (const storeType of ["memory", "sqlite"]) {
  describe(`Temporal authority: the store owns lease time (${storeType})`, () => {
    // --- M1 control: an injected clock that the store ignored would show up --
    test("TA-2: the store really reads its injected clock, not host time", () => {
      const { clock, adapters } = withClock(storeType);
      const reservation = reserveAt(adapters, clock, "clock-is-real");

      // Host time is ~1.7e12; a store on Date.now() cannot produce 6000.
      assert.equal(
        reservation.record.leaseExpiresAt,
        EXPIRY,
        "the reservation deadline must come from the injected store clock"
      );
      assert.ok(
        Date.now() - EXPIRY > 1e9,
        "control: host time is nowhere near the injected clock"
      );
      adapters.close();
    });

    // --- CT-1: caller-supplied time cannot buy a takeover --------------------
    test("CT-1/TA-3: an artificial future `now` cannot take over a live lease", () => {
      const { clock, adapters } = withClock(storeType);
      reserveAt(adapters, clock, "ct-1");

      clock.ms = EXPIRY - 1; // the store says: still live
      const before = adapters.commandStore.get("ct-1");

      const outcome = adapters.commandStore.takeOverExpired({
        commandId: "ct-1",
        workerId: "worker-B",
        leaseTtlMs: TTL,
        expectedToken: 1,
        now: CALLER_FUTURE, // ignored: the caller does not own lease time
      });

      assert.deepEqual(outcome, { success: false, reason: "NOT_EXPIRED" });
      assert.deepEqual(
        adapters.commandStore.get("ct-1"),
        before,
        "a refused takeover must not touch the row"
      );
      assert.equal(adapters.commandStore.get("ct-1").leaseToken, 1, "no generation change");
      adapters.close();
    });

    // --- CT-2: caller-supplied time cannot buy a revocation ------------------
    test("CT-2/TA-3: an artificial future `now` cannot revoke a live lease", () => {
      const { clock, adapters } = withClock(storeType);
      const event = seedPartialCommit(adapters, clock, "ct-2");

      clock.ms = EXPIRY - 1; // the store says: still live
      const before = adapters.commandStore.get("ct-2");

      const outcome = adapters.commandStore.revokeExpired({
        commandId: "ct-2",
        expectedToken: 1,
        error: REVOKE_ERROR,
        now: CALLER_FUTURE, // ignored
      });

      assert.deepEqual(outcome, { success: false, reason: "NOT_EXPIRED" });
      const after = adapters.commandStore.get("ct-2");
      assert.equal(after.status, COMMAND_STATUSES.PROCESSING, "a live command stays live");
      assert.deepEqual(after, before, "a refused revocation must not touch the row");
      assert.equal(
        adapters.eventStore.getByCommandId("ct-2").length,
        1,
        "the committed event history is untouched"
      );
      assert.equal(event.eventId, adapters.eventStore.getByCommandId("ct-2")[0].eventId);
      adapters.close();
    });

    // --- CT-3: caller-supplied past time cannot stall a real challenge -------
    test("CT-3/TA-3: an artificial past `now` cannot block a genuine challenge", () => {
      const { clock, adapters } = withClock(storeType);
      reserveAt(adapters, clock, "ct-3");

      clock.ms = EXPIRY; // the store says: expired
      const outcome = adapters.commandStore.takeOverExpired({
        commandId: "ct-3",
        workerId: "worker-B",
        leaseTtlMs: TTL,
        expectedToken: 1,
        now: CALLER_PAST, // ignored
      });

      assert.equal(outcome.success, true, "the store clock decides, so the takeover stands");
      assert.equal(outcome.record.leaseToken, 2);
      adapters.close();
    });

    // --- reserve: the caller cannot choose the deadline ---------------------
    test("TA-3: a caller cannot create a born-expired or far-future lease", () => {
      const { clock, adapters } = withClock(storeType);

      const bornExpired = adapters.commandStore.reserve({
        commandId: "reserve-past",
        commandType: "CHECKOUT",
        payload: { ...PAYLOAD },
        workerId: "worker-A",
        leaseTtlMs: TTL,
        now: CALLER_PAST, // ignored
      });
      assert.equal(
        bornExpired.record.leaseExpiresAt,
        EXPIRY,
        "the deadline comes from the store clock, so the lease is not born expired"
      );
      assert.equal(
        adapters.commandStore.takeOverExpired({
          commandId: "reserve-past",
          workerId: "worker-B",
          leaseTtlMs: TTL,
          expectedToken: 1,
        }).reason,
        "NOT_EXPIRED",
        "a fresh lease must not be immediately challengeable"
      );

      const farFuture = adapters.commandStore.reserve({
        commandId: "reserve-future",
        commandType: "CHECKOUT",
        payload: { ...PAYLOAD },
        workerId: "worker-A",
        leaseTtlMs: TTL,
        now: CALLER_FUTURE, // ignored
      });
      assert.equal(
        farFuture.record.leaseExpiresAt,
        EXPIRY,
        "a caller cannot push its deadline beyond the store's own reckoning"
      );
      adapters.close();
    });

    // --- renew: the caller cannot choose the extension ----------------------
    test("TA-3: a caller cannot extend a lease to a time of its own choosing", () => {
      const { clock, adapters } = withClock(storeType);
      reserveAt(adapters, clock, "renew-cmd");

      clock.ms = 3000;
      const renewed = adapters.commandStore.renewLease({
        commandId: "renew-cmd",
        workerId: "worker-A",
        fencingToken: 1,
        leaseTtlMs: TTL,
        now: CALLER_FUTURE, // ignored
      });

      assert.equal(
        renewed.leaseExpiresAt,
        3000 + TTL,
        "the new deadline is the store's clock plus the TTL, nothing else"
      );
      assert.equal(adapters.commandStore.get("renew-cmd").leaseExpiresAt, 3000 + TTL);
      adapters.close();
    });

    // --- TA-4: the exact boundary, identical in both adapters ---------------
    describe("TA-4: expired iff now >= leaseExpiresAt", () => {
      for (const [label, at, expected] of [
        ["expiry - 1", EXPIRY - 1, false],
        ["expiry", EXPIRY, true],
        ["expiry + 1", EXPIRY + 1, true],
      ]) {
        test(`takeover at ${label} -> ${expected ? "expired" : "NOT_EXPIRED"}`, () => {
          const { clock, adapters } = withClock(storeType);
          reserveAt(adapters, clock, "boundary-takeover");

          clock.ms = at;
          const outcome = adapters.commandStore.takeOverExpired({
            commandId: "boundary-takeover",
            workerId: "worker-B",
            leaseTtlMs: TTL,
            expectedToken: 1,
          });

          assert.equal(outcome.success, expected);
          if (!expected) {
            assert.equal(outcome.reason, "NOT_EXPIRED");
            assert.equal(adapters.commandStore.get("boundary-takeover").leaseToken, 1);
          }
          adapters.close();
        });

        test(`revocation at ${label} -> ${expected ? "expired" : "NOT_EXPIRED"}`, () => {
          const { clock, adapters } = withClock(storeType);
          seedPartialCommit(adapters, clock, "boundary-revoke");

          clock.ms = at;
          const outcome = adapters.commandStore.revokeExpired({
            commandId: "boundary-revoke",
            expectedToken: 1,
            error: REVOKE_ERROR,
          });

          assert.equal(outcome.success, expected);
          assert.equal(
            adapters.commandStore.get("boundary-revoke").status,
            expected ? COMMAND_STATUSES.FAILED : COMMAND_STATUSES.PROCESSING
          );
          adapters.close();
        });
      }
    });
  });
}

// ===========================================================================
// Model B at the Coordinator boundary: the Coordinator chooses *which*
// challenge is meaningful; it has no lease clock with which to veto one.
// ===========================================================================
describe("Temporal authority: the coordinator has no lease clock", () => {
  test("TA-3: the engine refuses a lease clock rather than ignoring one", () => {
    const { adapters } = withClock("memory");
    assert.throws(
      () =>
        new RollbackEngine({
          eventStore: adapters.eventStore,
          commandStore: adapters.commandStore,
          snapshotStore: adapters.snapshotStore,
          stateRepository: adapters.stateRepository,
          now: () => 1234,
        }),
      (error) =>
        error instanceof TypeError && /lease clock belongs to the command store/.test(error.message),
      "a lease clock handed to the engine would control nothing, so it must be refused"
    );
    adapters.close();
  });

  test("TA-3: the coordinator refuses a lease clock rather than ignoring one", () => {
    const { adapters } = withClock("memory");
    assert.throws(
      () =>
        new CommandExecutionCoordinator({
          eventStore: adapters.eventStore,
          commandStore: adapters.commandStore,
          operationIdGenerator: () => randomUUID(),
          now: () => 1234,
        }),
      (error) =>
        error instanceof TypeError && /lease clock belongs to the command store/.test(error.message)
    );
    adapters.close();
  });

  for (const storeType of ["memory", "sqlite"]) {
    test(`TA-2: a legitimate challenge reaches the store and wins (${storeType})`, () => {
      const { clock, adapters } = withClock(storeType);
      reserveAt(adapters, clock, "coordinator-challenge", BIRTH, { ...ENGINE_PAYLOAD });

      // Nothing between the retry and the store can suppress this: the store's
      // clock is past the deadline, so the challenge must be put and must win.
      clock.ms = EXPIRY;
      const challenges = [];
      const realTakeover = adapters.commandStore.takeOverExpired.bind(adapters.commandStore);
      adapters.commandStore.takeOverExpired = (args) => {
        const outcome = realTakeover(args);
        challenges.push(outcome);
        return outcome;
      };

      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-B",
        leaseTtlMs: TTL,
        clock: () => new Date(clock.ms).toISOString(),
      });

      const result = engine.checkout(PAYLOAD, { commandId: "coordinator-challenge" });

      assert.equal(challenges.length, 1, "the store must have been challenged exactly once");
      assert.equal(challenges[0].success, true, "and the store must have granted it");
      assert.equal(result.status, "completed");
      assert.equal(
        adapters.commandStore.get("coordinator-challenge").status,
        COMMAND_STATUSES.COMPLETED
      );
      adapters.close();
    });
  }
});

// ===========================================================================
// TA-5: absolute deadlines are persisted state, so they survive a restart.
// ===========================================================================
describe("Temporal authority: persisted deadlines survive a restart", () => {
  test("TA-5: a deadline written by one store instance binds the next", () => {
    const dbPath = join(tmpdir(), `rollback-temporal-restart-${randomUUID()}.db`);

    const first = { ms: BIRTH };
    const before = makeAdapters("sqlite", first, dbPath);
    before.commandStore.reserve({
      commandId: "restart-cmd",
      commandType: "CHECKOUT",
      payload: { ...PAYLOAD },
      workerId: "worker-A",
      leaseTtlMs: TTL,
    });
    before.close();

    // A fresh store on the same database, standing just short of the deadline.
    const live = { ms: EXPIRY - 1 };
    const after = makeAdapters("sqlite", live, dbPath);
    assert.equal(after.commandStore.get("restart-cmd").leaseExpiresAt, EXPIRY);
    assert.equal(
      after.commandStore.takeOverExpired({
        commandId: "restart-cmd",
        workerId: "worker-B",
        leaseTtlMs: TTL,
        expectedToken: 1,
      }).reason,
      "NOT_EXPIRED",
      "the persisted deadline still protects the previous owner"
    );

    live.ms = EXPIRY;
    assert.equal(
      after.commandStore.takeOverExpired({
        commandId: "restart-cmd",
        workerId: "worker-B",
        leaseTtlMs: TTL,
        expectedToken: 1,
      }).success,
      true,
      "and it becomes challengeable at exactly the persisted instant"
    );
    after.close();
  });
});
