const test = require("node:test");
const { describe } = require("node:test");
const assert = require("node:assert/strict");
const { join } = require("node:path");
const { tmpdir } = require("node:os");
const { randomUUID } = require("node:crypto");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { COMMAND_STATUSES } = require("../src/application/storeContracts");

const TTL = 5000;
const PAYLOAD = { item: "Widget", quantity: 1, amount: 100 };
const REVOKE_ERROR = {
  code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
  message: "Committed command events were found without a completed command result.",
  eventCommitted: true,
  retrySafe: false,
  retryAction: "MANUAL_RESOLUTION_REQUIRED",
};

/**
 * `leaseNow` is the command store's lease clock. Tests that need a lease to
 * expire advance the variable it closes over; they cannot hand a chosen time to
 * an individual mutation any more.
 */
function createAdapters(storeType, leaseNow) {
  return storeType === "sqlite"
    ? createStorageAdapters({
        type: "sqlite",
        dbPath: join(tmpdir(), `rollback-truth-${randomUUID()}.db`),
        leaseNow,
      })
    : createStorageAdapters({ type: "memory", leaseNow });
}

/**
 * Runs a keyed checkout and lets a competitor act exactly once, at the n-th
 * invocation of a chosen store/event-store method. Returns what the losing
 * owner surfaced plus the state that was actually persisted.
 */
function raceAt({ storeType, hook, target = "commandStore", nth = 1, interfere, command = PAYLOAD }) {
  const base = Date.parse("2026-08-15T12:00:00.000Z");
  let clock = base;
  const adapters = createAdapters(storeType, () => clock);
  if (typeof adapters.eventStore.setNow === "function") {
    adapters.eventStore.setNow(() => clock);
  }

  const commandId = `race-${randomUUID().slice(0, 8)}`;
  const host = target === "eventStore" ? adapters.eventStore : adapters.commandStore;
  const real = host[hook].bind(host);
  let calls = 0;
  let interference = null;
  host[hook] = (...args) => {
    calls += 1;
    if (interference === null && calls === nth) {
      clock += 60_000; // the owner stalls; its lease expires
      interference = interfere(adapters, commandId);
    }
    return real(...args);
  };

  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    workerId: "worker-A",
    leaseTtlMs: TTL,
    clock: () => new Date(clock).toISOString(),
  });

  let surfaced = null;
  try {
    engine.checkout(command, { commandId });
  } catch (error) {
    surfaced = error;
  }

  return {
    adapters,
    commandId,
    surfaced,
    interference,
    record: adapters.commandStore.get(commandId),
    events: adapters.eventStore.getByCommandId(commandId),
  };
}

const revoke = (adapters, commandId) =>
  adapters.commandStore.revokeExpired({
    commandId,
    expectedToken: 1,
    error: REVOKE_ERROR,
  });

const takeover = (adapters, commandId) =>
  adapters.commandStore.takeOverExpired({
    commandId,
    workerId: "worker-B",
    leaseTtlMs: TTL,
    expectedToken: 1,
  });

for (const storeType of ["memory", "sqlite"]) {
  describe(`Truth reconciliation after a lost race (${storeType})`, () => {
    // --- F1-L1..L4: the competitor established a terminal truth ------------

    const revocationLossPoints = [
      ["F1-L1", "the owner loses at recordEvent after its append committed", { hook: "recordEvent", nth: 2 }],
      ["F1-L2", "the owner loses at renewLease on the next checkpoint", { hook: "renewLease", nth: 2 }],
      ["F1-L3", "the owner loses at append", { hook: "append", target: "eventStore", nth: 2 }],
      ["F1-L4", "the owner loses at complete after the last event", { hook: "complete", nth: 1 }],
    ];

    for (const [label, description, hookSpec] of revocationLossPoints) {
      test(`${label}: ${description} and is told the persisted truth`, () => {
        const run = raceAt({ storeType, ...hookSpec, interfere: revoke });

        assert.equal(run.interference.success, true, "the competitor must actually have won");
        assert.equal(run.record.status, COMMAND_STATUSES.FAILED);
        assert.equal(run.record.error.code, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");

        // The whole point: the caller is given the established truth, not a
        // persistence failure that never happened.
        assert.notEqual(run.surfaced, null, "the owner must surface an error");
        assert.equal(
          run.surfaced.code,
          "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
          `owner surfaced ${run.surfaced.code} instead of the persisted truth`
        );
        assert.equal(run.surfaced.retryAction, "MANUAL_RESOLUTION_REQUIRED");
        assert.equal(run.surfaced.eventCommitted, true);
        assert.deepEqual(
          run.surfaced.eventIds,
          run.record.eventRange.eventIds,
          "the surfaced error must carry the authoritative event range"
        );

        // Safety: nothing was mutated by the losing owner.
        assert.equal(run.record.leaseToken, 1, "revocation keeps the generation");
        assert.deepEqual(
          run.record.eventRange.eventIds,
          run.events.map((event) => event.eventId),
          "bookkeeping still matches the authoritative history"
        );

        // A retry now returns the same answer the owner just received.
        const engine = new RollbackEngine({
          eventStore: run.adapters.eventStore,
          commandStore: run.adapters.commandStore,
          snapshotStore: run.adapters.snapshotStore,
          stateRepository: run.adapters.stateRepository,
          workerId: "worker-C",
          leaseTtlMs: TTL,
        });
        assert.throws(
          () => engine.checkout(PAYLOAD, { commandId: run.commandId }),
          (error) => {
            assert.equal(error.code, run.surfaced.code, "first answer and retry answer must agree");
            return true;
          }
        );
        assert.equal(
          run.adapters.eventStore.getByCommandId(run.commandId).length,
          run.events.length,
          "no additional events"
        );

        run.adapters.close();
      });
    }

    // --- F1-L5: no terminal truth exists, so fencing stays the answer ------

    test("F1-L5: a zero-event takeover is reported as a generation loss, not as interrupted", () => {
      const run = raceAt({ storeType, hook: "renewLease", nth: 1, interfere: takeover });

      assert.equal(run.interference.success, true);
      assert.equal(run.record.status, COMMAND_STATUSES.PROCESSING, "no terminal truth exists");
      assert.equal(run.record.leaseToken, 2);
      assert.equal(run.events.length, 0);

      assert.notEqual(run.surfaced, null);
      assert.equal(run.surfaced.code, "FENCING_TOKEN_STALE");
      assert.equal(run.surfaced.retryAction, "ACQUIRE_NEW_LEASE");
      assert.notEqual(
        run.surfaced.code,
        "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
        "a takeover must never be dressed up as a terminal interruption"
      );
      run.adapters.close();
    });

    // --- F1-L6: generation loss discovered *by* the truth read -------------

    test("F1-L6: a zero-event persistence attempt that lost the generation reports fencing", () => {
      const base = Date.parse("2026-08-15T12:00:00.000Z");
      let clock = base;
      const adapters = createAdapters(storeType, () => clock);
      if (typeof adapters.eventStore.setNow === "function") {
        adapters.eventStore.setNow(() => clock);
      }
      const commandId = "l6-generation-loss";

      // A deterministic domain rejection persists its failure with zero events.
      // A competitor takes the expired zero-event reservation over first, so the
      // persistence attempt is refused for a generation reason - which only the
      // truth read can distinguish from a store outage.
      const realFail = adapters.commandStore.fail.bind(adapters.commandStore);
      let interfered = false;
      adapters.commandStore.fail = (...args) => {
        if (!interfered) {
          interfered = true;
          clock += 60_000;
          const outcome = adapters.commandStore.takeOverExpired({
            commandId,
            workerId: "worker-B",
            leaseTtlMs: TTL,
            expectedToken: 1,
          });
          assert.equal(outcome.success, true, "the competitor must actually have won");
        }
        return realFail(...args);
      };

      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-A",
        leaseTtlMs: TTL,
        clock: () => new Date(clock).toISOString(),
      });

      assert.throws(
        () => engine.deleteOrder(9999, "missing aggregate", { commandId }),
        (error) => {
          assert.equal(
            error.code,
            "FENCING_TOKEN_STALE",
            `a lost generation must be reported as such, got ${error.code}`
          );
          assert.equal(error.providedToken, 1);
          assert.equal(error.currentToken, 2);
          return true;
        }
      );

      // No terminal truth was fabricated and the competitor still owns the row.
      const row = adapters.commandStore.get(commandId);
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(row.leaseToken, 2);
      assert.equal(row.leaseOwner, "worker-B");
      assert.equal(row.error, null);
      adapters.close();
    });

    // --- Controls: the two answers that must NOT change -------------------

    test("control: a genuine persistence failure stays COMMAND_STATE_PERSISTENCE_FAILED", () => {
      const adapters = createAdapters(storeType);
      const commandId = "control-persist";

      // No competitor at all. fail() breaks once for a real reason while the row
      // is still processing under this worker's own generation.
      const realFail = adapters.commandStore.fail.bind(adapters.commandStore);
      let broken = true;
      adapters.commandStore.fail = (...args) => {
        if (broken) {
          broken = false;
          throw new Error("command failure store unavailable");
        }
        return realFail(...args);
      };

      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-A",
        leaseTtlMs: TTL,
      });

      assert.throws(
        () => engine.deleteOrder(9999, "missing aggregate", { commandId }),
        (error) => {
          assert.equal(error.code, "COMMAND_STATE_PERSISTENCE_FAILED");
          assert.equal(error.retryAction, "RECONCILE_SAME_KEY");
          return true;
        }
      );

      const row = adapters.commandStore.get(commandId);
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING, "own generation still holds the row");
      assert.equal(row.leaseToken, 1);
      adapters.close();
    });

    test("control: the truth read failing does not hide the persistence failure", () => {
      const adapters = createAdapters(storeType);
      const commandId = "control-blind";

      const realFail = adapters.commandStore.fail.bind(adapters.commandStore);
      const realGet = adapters.commandStore.get.bind(adapters.commandStore);
      let failed = false;
      adapters.commandStore.fail = () => {
        failed = true;
        throw new Error("command failure store unavailable");
      };
      adapters.commandStore.get = (...args) => {
        if (failed) throw new Error("command store unreadable");
        return realGet(...args);
      };

      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-A",
        leaseTtlMs: TTL,
      });

      assert.throws(
        () => engine.deleteOrder(9999, "missing aggregate", { commandId }),
        (error) => {
          assert.equal(error.code, "COMMAND_STATE_PERSISTENCE_FAILED");
          return true;
        }
      );
      adapters.commandStore.get = realGet;
      adapters.commandStore.fail = realFail;
      adapters.close();
    });

    // --- The release path keeps the domain error as the primary cause ------

    test("release path: the original domain error survives a lost reservation", () => {
      const base = Date.parse("2026-08-15T12:00:00.000Z");
      let clock = base;
      const adapters = createAdapters(storeType, () => clock);
      if (typeof adapters.eventStore.setNow === "function") {
        adapters.eventStore.setNow(() => clock);
      }
      const commandId = "release-loss";

      // A zero-event command fails with a domain error while a competitor takes
      // the expired reservation over.
      const realGetLastSequence = adapters.eventStore.getLastSequence.bind(adapters.eventStore);
      let fired = false;
      adapters.eventStore.getLastSequence = (aggregateId) => {
        if (!fired) {
          fired = true;
          clock += 60_000;
          adapters.commandStore.takeOverExpired({
            commandId,
            workerId: "worker-B",
            leaseTtlMs: TTL,
            expectedToken: 1,
          });
          throw Object.assign(new Error("inventory service exploded"), {
            code: "INVENTORY_UNAVAILABLE",
          });
        }
        return realGetLastSequence(aggregateId);
      };

      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: "worker-A",
        leaseTtlMs: TTL,
        clock: () => new Date(clock).toISOString(),
      });

      assert.throws(
        () => engine.checkout(PAYLOAD, { commandId }),
        (error) => {
          assert.equal(
            error.code,
            "INVENTORY_UNAVAILABLE",
            `the domain cause must stay primary, got ${error.code}`
          );
          assert.equal(
            error.cause?.code,
            "FENCING_TOKEN_STALE",
            "the lease loss must be attached as a technical cause"
          );
          return true;
        }
      );

      // The reservation still belongs to the competitor.
      const row = adapters.commandStore.get(commandId);
      assert.equal(row.status, COMMAND_STATUSES.PROCESSING);
      assert.equal(row.leaseToken, 2);
      assert.equal(row.leaseOwner, "worker-B");
      adapters.close();
    });
  });
}
