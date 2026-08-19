const test = require("node:test");
const { describe } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const {
  CommandExecutionCoordinator,
} = require("../src/application/commandExecutionCoordinator");

const PAYLOAD = { item: "Widget", quantity: 1, amount: 100 };
/** The shape the engine itself normalises a checkout command into. */
const ENGINE_PAYLOAD = { ...PAYLOAD, simulateFailureAt: null };

const INVALID_LEASE_TTLS = [
  ["null", null],
  ["0", 0],
  ["-1", -1],
  ["0.5", 0.5],
  ["NaN", NaN],
  ["Infinity", Infinity],
  ['"5000"', "5000"],
  ["{}", {}],
];

function coordinatorOn(adapters, leaseTtlMs) {
  return new CommandExecutionCoordinator({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    operationIdGenerator: () => randomUUID(),
    ...(leaseTtlMs === undefined ? {} : { leaseTtlMs }),
  });
}

describe("Lease policy configuration", () => {
  // A lease duration that is silently replaced by a working default is the
  // same failure mode the lease clock had: a control that looks authoritative
  // and decides nothing. Misconfiguration must be visible at wiring time.
  test("LT-5: an explicitly invalid lease duration is refused, not replaced", () => {
    for (const [label, leaseTtlMs] of INVALID_LEASE_TTLS) {
      const adapters = createStorageAdapters({ type: "memory" });
      assert.throws(
        () => coordinatorOn(adapters, leaseTtlMs),
        TypeError,
        `the coordinator must refuse a lease duration of ${label}`
      );
      adapters.close();
    }
  });

  test("LT-5: the engine refuses an explicitly invalid lease duration too", () => {
    for (const [label, leaseTtlMs] of INVALID_LEASE_TTLS) {
      const adapters = createStorageAdapters({ type: "memory" });
      assert.throws(
        () =>
          new RollbackEngine({
            eventStore: adapters.eventStore,
            commandStore: adapters.commandStore,
            snapshotStore: adapters.snapshotStore,
            stateRepository: adapters.stateRepository,
            leaseTtlMs,
          }),
        TypeError,
        `the engine must refuse a lease duration of ${label}`
      );
      adapters.close();
    }
  });

  // Removing the silent fallback must not move any valid-path default.
  test("valid-path defaults are unchanged by the policy check", () => {
    const adapters = createStorageAdapters({ type: "memory" });
    assert.equal(coordinatorOn(adapters, undefined) instanceof CommandExecutionCoordinator, true);
    assert.equal(coordinatorOn(adapters, 5000) instanceof CommandExecutionCoordinator, true);
    adapters.close();

    // The engine's own default still reaches the store as 30000.
    const clock = { ms: 1000 };
    const engineAdapters = createStorageAdapters({
      type: "memory",
      leaseNow: () => clock.ms,
    });
    let reservedWith = null;
    const realReserve = engineAdapters.commandStore.reserve.bind(engineAdapters.commandStore);
    engineAdapters.commandStore.reserve = (args) => {
      reservedWith = args.leaseTtlMs;
      return realReserve(args);
    };
    const engine = new RollbackEngine({
      eventStore: engineAdapters.eventStore,
      commandStore: engineAdapters.commandStore,
      snapshotStore: engineAdapters.snapshotStore,
      stateRepository: engineAdapters.stateRepository,
      workerId: "worker-default",
    });
    engine.checkout(PAYLOAD, { commandId: "default-ttl" });
    assert.equal(reservedWith, 30000, "the engine default must still be 30000");
    engineAdapters.close();
  });

  test("LT-4: a valid lease duration reaches reserve, renewal and takeover unchanged", () => {
    const clock = { ms: 1000 };
    const adapters = createStorageAdapters({
      type: "memory",
      leaseNow: () => clock.ms,
    });
    const commandId = "policy-roundtrip";

    // A previous owner reserves and dies; its lease runs out at 2000.
    adapters.commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: { ...ENGINE_PAYLOAD },
      workerId: "worker-A",
      leaseTtlMs: 1000,
    });

    const seen = { reserve: [], renewLease: [], takeOverExpired: [] };
    for (const method of ["reserve", "renewLease", "takeOverExpired"]) {
      const real = adapters.commandStore[method].bind(adapters.commandStore);
      adapters.commandStore[method] = (args) => {
        seen[method].push(args.leaseTtlMs);
        return real(args);
      };
    }

    clock.ms = 9000;
    const engine = new RollbackEngine({
      eventStore: adapters.eventStore,
      commandStore: adapters.commandStore,
      snapshotStore: adapters.snapshotStore,
      stateRepository: adapters.stateRepository,
      workerId: "worker-B",
      leaseTtlMs: 7000,
    });

    const result = engine.checkout(PAYLOAD, { commandId });
    assert.equal(result.status, "completed");

    assert.deepEqual(seen.reserve, [7000], "reserve must receive the configured policy");
    assert.equal(seen.takeOverExpired.length, 1, "the takeover must have happened");
    assert.deepEqual(seen.takeOverExpired, [7000], "takeover must receive the configured policy");
    assert.ok(seen.renewLease.length > 0, "at least one renewal must have happened");
    assert.deepEqual(
      [...new Set(seen.renewLease)],
      [7000],
      "every renewal must receive the configured policy"
    );

    adapters.close();
  });
});
