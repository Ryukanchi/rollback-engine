const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");

const CHECKOUT = Object.freeze({
  item: "Widget",
  quantity: 2,
  amount: 250,
});
const FIXED_TIMESTAMP = "2026-08-20T12:00:00.000Z";

function createHarness(storeType) {
  const adapters = createStorageAdapters({ type: storeType });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    workerId: `f4-${storeType}`,
    clock: () => FIXED_TIMESTAMP,
  });

  return { adapters, engine };
}

function forgedRolledBackState(state) {
  return {
    ...state,
    lifecycle: "rolled_back",
    order: { ...state.order, status: "rolled_back" },
    inventory: { ...state.inventory, status: "released" },
    payment: { ...state.payment, status: "refunded" },
  };
}

function injectViewAfterVersion(stateRepository, version, forge) {
  const realCompareAndSwap = stateRepository.compareAndSwap.bind(stateRepository);
  let injected = false;

  stateRepository.compareAndSwap = (args) => {
    const outcome = realCompareAndSwap(args);

    if (!injected && outcome.applied && args.nextState.version === version) {
      injected = true;
      stateRepository.replace(forge(args.nextState));
    }

    return outcome;
  };

  return {
    wasInjected: () => injected,
    restore: () => {
      stateRepository.compareAndSwap = realCompareAndSwap;
    },
  };
}

for (const storeType of ["memory", "sqlite"]) {
  test(`F-4: a rolled-back view cannot suppress checkout compensation (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const injection = injectViewAfterVersion(
      adapters.stateRepository,
      3,
      forgedRolledBackState
    );
    t.after(injection.restore);

    const result = engine.checkout(
      { ...CHECKOUT, simulateFailureAt: "after_payment" },
      { commandId: `f4-saga-${storeType}` }
    );
    const events = adapters.eventStore.getByAggregateId(result.aggregateId);
    const replay = engine.replay(result.aggregateId);
    const command = adapters.commandStore.get(`f4-saga-${storeType}`);

    assert.equal(injection.wasInjected(), true);
    assert.deepEqual(
      events.map((event) => event.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
        EVENT_TYPES.PAYMENT_REFUNDED,
        EVENT_TYPES.INVENTORY_RELEASED,
        EVENT_TYPES.ORDER_ROLLED_BACK,
      ]
    );
    assert.equal(result.status, "rolled_back");
    assert.deepEqual(result.state, replay);
    assert.deepEqual(result.snapshot.state, replay);
    assert.equal(command.status, "completed");
    assert.deepEqual(command.result.state, replay);
  });

  test(`F-4: a view-only refund cannot enter a completed command result (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const injection = injectViewAfterVersion(
      adapters.stateRepository,
      3,
      (state) => ({
        ...state,
        payment: { ...state.payment, status: "refunded" },
      })
    );
    t.after(injection.restore);

    const commandId = `f4-result-${storeType}`;
    const result = engine.checkout(CHECKOUT, { commandId });
    const events = adapters.eventStore.getByAggregateId(result.aggregateId);
    const replay = engine.replay(result.aggregateId);
    const command = adapters.commandStore.get(commandId);

    assert.equal(injection.wasInjected(), true);
    assert.equal(
      events.some((event) => event.eventType === EVENT_TYPES.PAYMENT_REFUNDED),
      false
    );
    assert.equal(replay.payment.status, "charged");
    assert.deepEqual(result.state, replay);
    assert.deepEqual(result.snapshot.state, replay);
    assert.equal(command.status, "completed");
    assert.deepEqual(command.result.state, replay);
  });

  test(`F-4: unreconcilable saga state fails closed before command completion (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const stateRepository = adapters.stateRepository;
    const realCompareAndSwap = stateRepository.compareAndSwap.bind(stateRepository);
    const realComplete = adapters.commandStore.complete.bind(adapters.commandStore);
    let sabotageRepair = false;
    let casLosses = 0;
    let completeCalls = 0;

    stateRepository.compareAndSwap = (args) => {
      if (sabotageRepair) {
        casLosses += 1;
        const current = stateRepository.getByAggregateId(args.aggregateId);
        stateRepository.replace({ ...current, corruptionNonce: casLosses });
        return realCompareAndSwap(args);
      }

      const outcome = realCompareAndSwap(args);
      if (outcome.applied && args.nextState.version === 3) {
        stateRepository.replace(forgedRolledBackState(args.nextState));
        sabotageRepair = true;
      }
      return outcome;
    };
    adapters.commandStore.complete = (...args) => {
      completeCalls += 1;
      return realComplete(...args);
    };

    const commandId = `f4-cas-${storeType}`;
    let failure = null;
    assert.throws(
      () =>
        engine.checkout(
          { ...CHECKOUT, simulateFailureAt: "after_payment" },
          { commandId }
        ),
      (error) => {
        failure = error;
        return (
          error.code === "COMMAND_EXECUTION_PARTIALLY_COMMITTED" &&
          error.cause?.code === "MATERIALIZED_VIEW_RECONCILIATION_EXHAUSTED"
        );
      }
    );

    const events = adapters.eventStore.getByAggregateId(1);
    const replay = engine.replay(1);
    const command = adapters.commandStore.get(commandId);

    assert.ok(casLosses > 1, "authoritative reconciliation must be attempted and bounded");
    assert.equal(completeCalls, 0);
    assert.equal(failure.eventCommitted, true);
    assert.deepEqual(
      events.map((event) => event.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
      ]
    );
    assert.equal(replay.lifecycle, "completed");
    assert.equal(replay.payment.status, "charged");
    assert.equal(command.status, "failed");
    assert.equal(command.result, null);
  });

  test(`F-4: replay, snapshot, authoritative read, and command result agree (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const commandId = `f4-consistency-${storeType}`;

    const result = engine.checkout(CHECKOUT, { commandId });
    const replay = engine.replay(result.aggregateId);
    const authoritative = engine.getState(result.aggregateId, {
      consistency: "authoritative",
    });
    const persistedCommand = adapters.commandStore.get(commandId);

    assert.deepEqual(result.state, replay);
    assert.deepEqual(result.snapshot.state, replay);
    assert.deepEqual(authoritative, replay);
    assert.deepEqual(persistedCommand.result.state, replay);
  });

  test(`F-4: command completion revalidates result state against latest replay (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const realSnapshotSave = adapters.snapshotStore.save.bind(
      adapters.snapshotStore
    );
    const realComplete = adapters.commandStore.complete.bind(
      adapters.commandStore
    );
    let competitorAppended = false;
    let completeCalls = 0;

    adapters.snapshotStore.save = (snapshot) => {
      const storedSnapshot = realSnapshotSave(snapshot);

      if (!competitorAppended && snapshot.aggregateId === 1 && snapshot.version === 3) {
        competitorAppended = true;
        adapters.eventStore.append(
          createDomainEvent({
            eventId: `f4-competitor-${storeType}`,
            eventType: EVENT_TYPES.PAYMENT_REFUNDED,
            aggregateId: 1,
            sequence: 4,
            timestamp: FIXED_TIMESTAMP,
            payload: {
              paymentId: 1,
              reason: "concurrent authoritative event",
            },
            metadata: {
              schemaVersion: 1,
              commandId: `f4-competitor-command-${storeType}`,
              correlationId: `f4-competitor-command-${storeType}`,
              causationId: `f4-competitor-command-${storeType}`,
            },
          }),
          { expectedVersion: 3 }
        );
      }

      return storedSnapshot;
    };
    adapters.commandStore.complete = (...args) => {
      completeCalls += 1;
      return realComplete(...args);
    };

    const commandId = `f4-complete-boundary-${storeType}`;
    let failure = null;
    assert.throws(
      () => engine.checkout(CHECKOUT, { commandId }),
      (error) => {
        failure = error;
        return (
          error.code === "COMMAND_EXECUTION_PARTIALLY_COMMITTED" &&
          error.cause?.code === "COMMAND_RESULT_STATE_NOT_AUTHORITATIVE"
        );
      }
    );

    const replay = engine.replay(1);
    const command = adapters.commandStore.get(commandId);
    assert.equal(competitorAppended, true);
    assert.equal(completeCalls, 0);
    assert.equal(failure.eventCommitted, true);
    assert.equal(replay.version, 4);
    assert.equal(replay.payment.status, "refunded");
    assert.equal(command.status, "failed");
    assert.equal(command.result, null);
    assert.equal(adapters.snapshotStore.getByAggregateId(1).version, 3);
  });
}
