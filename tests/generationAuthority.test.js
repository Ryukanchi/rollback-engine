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
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { COMMAND_STATUSES } = require("../src/application/storeContracts");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");

const PAYLOAD = { item: "Widget", quantity: 1, amount: 100 };

function createEvent({ commandId, aggregateId = 1, sequence = 1, eventId }) {
  return createDomainEvent({
    eventId: eventId || `evt-${randomUUID()}`,
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId,
    sequence,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: PAYLOAD.item, quantity: PAYLOAD.quantity },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });
}

function createAdapters(type) {
  if (type === "sqlite") {
    const dbPath = join(tmpdir(), `rollback-generation-${randomUUID()}.db`);
    return createStorageAdapters({ type: "sqlite", dbPath });
  }
  return createStorageAdapters({ type: "memory" });
}

function reserve(store, commandId, { workerId = "worker-1", leaseTtlMs = 1000, now = 1000 } = {}) {
  return store.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { ...PAYLOAD },
    workerId,
    leaseTtlMs,
    now,
  });
}

function isStaleGeneration(error, { provided, current }) {
  assert.equal(
    error.code,
    "FENCING_TOKEN_STALE",
    `expected a generation rejection, got ${error.code}: ${error.message}`
  );
  assert.equal(error.providedToken, provided, "rejection must name the stale generation");
  assert.equal(error.currentToken, current, "rejection must name the current generation");
  return true;
}

// ---------------------------------------------------------------------------
// The whole contract runs against both stores. Memory and SQLite must not drift.
// ---------------------------------------------------------------------------
for (const storeType of ["memory", "sqlite"]) {
  describe(`Generation authority (${storeType})`, () => {
    // === G1: a generation number is never handed out twice ===================

    test("releaseFailed retains the row so the generation survives", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g1-retain";

      reserve(store, commandId);
      store.fail(commandId, { code: "EVENT_APPEND_COMMIT_UNKNOWN" }, { fencingToken: 1 });

      assert.equal(
        store.releaseFailed(commandId, "EVENT_APPEND_COMMIT_UNKNOWN", {
          fencingToken: 1,
        }),
        true
      );

      const retained = store.get(commandId);
      assert.notEqual(retained, null, "the row must not be deleted");
      assert.equal(retained.status, COMMAND_STATUSES.RELEASED);
      assert.equal(retained.leaseToken, 1, "the spent generation is preserved");
      assert.equal(retained.leaseOwner, null);
      assert.equal(retained.leaseExpiresAt, null);

      adapters.close();
    });

    test("re-reservation after releaseFailed issues a strictly newer generation", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g1-successor";

      reserve(store, commandId);
      store.fail(commandId, { code: "EVENT_APPEND_COMMIT_UNKNOWN" }, { fencingToken: 1 });
      store.releaseFailed(commandId, "EVENT_APPEND_COMMIT_UNKNOWN", { fencingToken: 1 });

      const reReservation = reserve(store, commandId, { workerId: "worker-2", now: 5000 });

      assert.equal(reReservation.created, true);
      assert.equal(reReservation.record.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(reReservation.record.leaseOwner, "worker-2");
      assert.equal(
        reReservation.record.leaseToken,
        2,
        "the failed generation must not be reissued"
      );

      adapters.close();
    });

    test("generations stay monotonic across takeover, failure and re-reservation", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g1-monotonic";
      const seen = [];

      seen.push(reserve(store, commandId, { workerId: "A", leaseTtlMs: 10, now: 1000 }).record.leaseToken);
      seen.push(store.takeOverExpired({ commandId, workerId: "B", leaseTtlMs: 10, now: 5000 }).record.leaseToken);
      seen.push(store.takeOverExpired({ commandId, workerId: "C", leaseTtlMs: 10, now: 9000 }).record.leaseToken);

      store.fail(commandId, { code: "EVENT_APPEND_COMMIT_UNKNOWN" }, { fencingToken: 3 });
      store.releaseFailed(commandId, "EVENT_APPEND_COMMIT_UNKNOWN", { fencingToken: 3 });
      seen.push(reserve(store, commandId, { workerId: "D", leaseTtlMs: 60000, now: 20000 }).record.leaseToken);

      assert.deepEqual(seen, [1, 2, 3, 4]);

      // Every spent generation is refused against the live one. The row is
      // processing with an unexpired lease and no committed events, so nothing
      // except the generation can be the reason.
      for (const staleToken of [1, 2, 3]) {
        assert.throws(
          () => store.complete(commandId, { hijacked: staleToken }, { fencingToken: staleToken }),
          (error) => isStaleGeneration(error, { provided: staleToken, current: 4 })
        );
      }

      assert.equal(store.get(commandId).status, COMMAND_STATUSES.PROCESSING);
      assert.equal(store.get(commandId).result, null);

      // The identical call from the live generation is accepted, which proves
      // the rejections above were about the generation and nothing else.
      assert.equal(
        store.complete(commandId, { hijacked: 4 }, { fencingToken: 4 }).status,
        COMMAND_STATUSES.COMPLETED
      );

      adapters.close();
    });

    // === G2/G3: a stale generation cannot mutate the live one ===============

    for (const mutation of ["complete", "fail", "release", "recordEvent"]) {
      test(`${mutation}() from a stale generation is rejected for the generation, not the state`, () => {
        const adapters = createAdapters(storeType);
        const store = adapters.commandStore;
        const commandId = `g2-${mutation}`;

        reserve(store, commandId, { workerId: "A", leaseTtlMs: 10, now: 1000 });
        const takeover = store.takeOverExpired({
          commandId,
          workerId: "B",
          leaseTtlMs: 60000,
          now: 5000,
        });
        assert.equal(takeover.record.leaseToken, 2);
        assert.equal(takeover.record.status, COMMAND_STATUSES.PROCESSING);

        const invoke = (token) => {
          if (mutation === "complete") return store.complete(commandId, { ok: true }, { fencingToken: token });
          if (mutation === "fail") return store.fail(commandId, { code: "X" }, { fencingToken: token });
          if (mutation === "release") return store.release(commandId, { fencingToken: token });
          return store.recordEvent(commandId, createEvent({ commandId }), { fencingToken: token });
        };

        assert.throws(
          () => invoke(1),
          (error) => isStaleGeneration(error, { provided: 1, current: 2 })
        );

        const untouched = store.get(commandId);
        assert.equal(untouched.status, COMMAND_STATUSES.PROCESSING);
        assert.equal(untouched.leaseOwner, "B");
        assert.equal(untouched.leaseToken, 2);
        assert.equal(untouched.eventRange, null);
        assert.equal(untouched.result, null);
        assert.equal(untouched.error, null);

        // Same call, live generation: accepted, and it actually lands. The
        // precondition was satisfied all along, so the stale rejection cannot
        // have been a state rejection.
        assert.doesNotThrow(() => invoke(2));

        const applied = store.get(commandId);
        assert.notEqual(applied, null, "the live mutation must keep the row");
        const expectedStatus = {
          complete: COMMAND_STATUSES.COMPLETED,
          fail: COMMAND_STATUSES.FAILED,
          release: COMMAND_STATUSES.RELEASED,
          recordEvent: COMMAND_STATUSES.PROCESSING,
        }[mutation];
        assert.equal(applied.status, expectedStatus);
        assert.equal(applied.leaseToken, 2, "the live generation is preserved");
        if (mutation === "recordEvent") {
          assert.equal(applied.eventRange.eventIds.length, 1);
        }

        adapters.close();
      });
    }

    // === G4: reconciliation is generation-scoped, but not owner-scoped ======

    test("reconcileEvents() from a stale generation cannot rewrite live bookkeeping", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g4-events";

      reserve(store, commandId, { workerId: "A", leaseTtlMs: 10, now: 1000 });
      store.takeOverExpired({ commandId, workerId: "B", leaseTtlMs: 60000, now: 5000 });

      const forgedEvents = [createEvent({ commandId, aggregateId: 42, sequence: 7 })];

      assert.throws(
        () => store.reconcileEvents(commandId, forgedEvents, { fencingToken: 1 }),
        (error) => isStaleGeneration(error, { provided: 1, current: 2 })
      );
      assert.equal(store.get(commandId).eventRange, null);
      assert.equal(store.get(commandId).aggregateId, null);

      // Identical repair from the live generation is accepted.
      const repaired = store.reconcileEvents(commandId, forgedEvents, { fencingToken: 2 });
      assert.equal(repaired.eventRange.firstSequence, 7);

      adapters.close();
    });

    test("reconcileFailure() from a stale generation cannot rewrite range, error or status", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g4-failure";

      // Generation 1 fails, generation 2 takes the row over and fails as well,
      // so the row is FAILED and the live generation is 2.
      reserve(store, commandId, { workerId: "A", leaseTtlMs: 10, now: 1000 });
      store.takeOverExpired({ commandId, workerId: "B", leaseTtlMs: 60000, now: 5000 });
      store.fail(
        commandId,
        { code: "EVENT_APPEND_COMMIT_UNKNOWN", message: "written by generation 2" },
        { fencingToken: 2 }
      );

      const before = store.get(commandId);

      assert.throws(
        () =>
          store.reconcileFailure(
            commandId,
            [createEvent({ commandId, aggregateId: 99, sequence: 3 })],
            { code: "FORGED_BY_GENERATION_1" },
            { fencingToken: 1 }
          ),
        (error) => isStaleGeneration(error, { provided: 1, current: 2 })
      );

      const after = store.get(commandId);
      assert.equal(after.status, before.status);
      assert.equal(after.error.code, "EVENT_APPEND_COMMIT_UNKNOWN");
      assert.equal(after.error.message, "written by generation 2");
      assert.equal(after.eventRange, before.eventRange);
      assert.equal(after.aggregateId, before.aggregateId);

      adapters.close();
    });

    test("reconcileFailure() still repairs bookkeeping for a recovering worker that never held the lease", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g4-recovery";
      const event = createEvent({ commandId, aggregateId: 5, sequence: 1 });

      reserve(store, commandId, { workerId: "A", leaseTtlMs: 10, now: 1000 });
      store.fail(commandId, { code: "EVENT_APPEND_COMMIT_UNKNOWN" }, { fencingToken: 1 });

      // The row now has no lease owner at all. A recovering worker observes
      // generation 1 and repairs the bookkeeping from the event history.
      assert.equal(store.get(commandId).leaseOwner, null);

      const repaired = store.reconcileFailure(
        commandId,
        [event],
        { code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" },
        { fencingToken: 1 }
      );

      assert.equal(repaired.status, COMMAND_STATUSES.FAILED);
      assert.equal(repaired.aggregateId, 5);
      assert.deepEqual(repaired.eventRange.eventIds, [event.eventId]);
      assert.equal(repaired.error.code, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");

      adapters.close();
    });

    // === G2 end to end: the eventless-command hijack ========================

    test("a zombie of a resolved generation cannot complete the retry that replaced it", () => {
      const adapters = createAdapters(storeType);
      const store = adapters.commandStore;
      const commandId = "g2-e2e";

      // Generation 1 dies with an unknown commit and no events.
      reserve(store, commandId, { workerId: "A", leaseTtlMs: 60000, now: 1000 });
      store.fail(commandId, { code: "EVENT_APPEND_COMMIT_UNKNOWN" }, { fencingToken: 1 });

      // A recovering worker resolves the row and retries under a new generation.
      store.releaseFailed(commandId, "EVENT_APPEND_COMMIT_UNKNOWN", { fencingToken: 1 });
      const retry = reserve(store, commandId, { workerId: "C", leaseTtlMs: 60000, now: 9000 });
      assert.equal(retry.record.leaseToken, 2);

      // Zombie A wakes up. Its command produced no events, so it never passes
      // through renewLease() and reaches complete() directly.
      assert.throws(
        () => store.complete(commandId, { hijacked: true }, { fencingToken: 1 }),
        (error) => isStaleGeneration(error, { provided: 1, current: 2 })
      );

      const row = store.get(commandId);
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(row.leaseOwner, "C");
      assert.equal(row.result, null);

      adapters.close();
    });
  });
}

// ---------------------------------------------------------------------------
// G5: SQLite must be atomic against a generation change. These two tests cover
// the two independent layers of the guarantee.
// ---------------------------------------------------------------------------
describe("Generation authority (SQLite atomicity)", () => {
  /**
   * Test-only wrapper that fires a barrier immediately after the store's
   * validating SELECT has resolved, i.e. exactly in the window between
   * validation and mutation. No production code is involved.
   */
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

  test("a generation change inside the validation window makes the stale UPDATE match no row", () => {
    const dbPath = join(tmpdir(), `rollback-cas-${randomUUID()}.db`);
    const db = createSqliteDatabase({ path: dbPath });
    const commandId = "cas-cmd";

    let armed = false;
    const store = new SqliteCommandStore({
      db: withBarrierAfterSelect(db, () => {
        if (!armed) return;
        armed = false;
        // The generation moves on after the store validated it but before it
        // writes. This is the interleaving F3 exploited.
        db.prepare("UPDATE commands SET lease_token = lease_token + 1 WHERE command_id = ?").run(
          commandId
        );
      }),
    });

    try {
      reserve(store, commandId, { workerId: "A", leaseTtlMs: 60000, now: 1000 });

      armed = true;
      assert.throws(
        () => store.complete(commandId, { hijacked: true }, { fencingToken: 1 }),
        (error) => {
          assert.equal(error.code, "FENCING_TOKEN_STALE");
          return true;
        }
      );

      // Nothing the stale actor did survived: the row is neither completed nor
      // carrying its result.
      const row = store.get(commandId);
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(row.result, null);
    } finally {
      db.close();
    }
  });

  test("a second connection cannot take over inside a mutation transaction", () => {
    const dbPath = join(tmpdir(), `rollback-serialize-${randomUUID()}.db`);
    // A short busy timeout keeps the contention deterministic and fast.
    const dbA = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
    const dbB = createSqliteDatabase({ path: dbPath, busyTimeout: 50 });
    const commandId = "serialize-cmd";
    const storeB = new SqliteCommandStore({ db: dbB });

    let armed = false;
    let takeoverOutcome = null;
    const storeA = new SqliteCommandStore({
      db: withBarrierAfterSelect(dbA, () => {
        if (!armed) return;
        armed = false;
        try {
          takeoverOutcome = storeB.takeOverExpired({
            commandId,
            workerId: "B",
            leaseTtlMs: 60000,
            now: 50_000,
            expectedToken: 1,
          });
        } catch (error) {
          takeoverOutcome = { success: false, reason: "BLOCKED", error };
        }
      }),
    });

    try {
      reserve(storeA, commandId, { workerId: "A", leaseTtlMs: 1000, now: 1000 });

      armed = true;
      let mutationError = null;
      try {
        storeA.complete(commandId, { byA: true }, { fencingToken: 1 });
      } catch (error) {
        mutationError = error;
      }

      assert.notEqual(
        takeoverOutcome,
        null,
        "the barrier must have run inside the mutation window"
      );
      assert.notEqual(
        takeoverOutcome.success,
        true,
        "a competing generation must not be able to commit inside the window"
      );

      // Whichever way the contention resolves, the row is never left in the
      // corrupt shape F3 produced: a terminal status stamped by generation 1
      // while the lease already belongs to generation 2.
      const row = storeB.get(commandId);
      const clobbered = row.status !== COMMAND_STATUSES.PROCESSING && row.leaseToken !== 1;
      assert.equal(clobbered, false, `row was clobbered: ${JSON.stringify(row)}`);

      if (mutationError === null) {
        assert.equal(row.status, COMMAND_STATUSES.COMPLETED);
        assert.equal(row.leaseToken, 1);
      } else {
        assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
        assert.equal(row.leaseToken, 1);
      }
    } finally {
      dbA.close();
      dbB.close();
    }
  });
});

// ---------------------------------------------------------------------------
// The engine keeps working end to end under the stricter contract.
// ---------------------------------------------------------------------------
describe("Generation authority (engine integration)", () => {
  for (const storeType of ["memory", "sqlite"]) {
    test(`a full checkout still completes under the mandatory generation contract (${storeType})`, () => {
      const adapters = createAdapters(storeType);
      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-1",
      });

      const result = engine.checkout(PAYLOAD, { commandId: "engine-ok" });
      assert.equal(result.status, "completed");

      const record = adapters.commandStore.get("engine-ok");
      assert.equal(record.status, COMMAND_STATUSES.COMPLETED);
      assert.equal(record.leaseToken, 1);
      assert.equal(record.leaseOwner, null);
      assert.deepEqual(record.eventRange.eventIds.length, 3);

      // Replaying the same key deduplicates rather than re-executing.
      const replayed = engine.checkout(PAYLOAD, { commandId: "engine-ok" });
      assert.deepEqual(replayed.aggregateId, result.aggregateId);
      assert.equal(adapters.eventStore.getAll().length, 3);

      adapters.close();
    });
  }
});
