const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { FAILURE_POINTS } = require("../src/domain/checkoutSaga");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const {
  InMemorySnapshotStore,
} = require("../src/infrastructure/inMemorySnapshotStore");
const {
  InMemoryStateRepository,
} = require("../src/infrastructure/inMemoryStateRepository");

function createRecoveryHarness() {
  const eventStore = new InMemoryEventStore();
  const stateRepository = new InMemoryStateRepository();
  const snapshotStore = new InMemorySnapshotStore();
  let eventId = 0;
  let timestamp = 0;
  const engine = new RollbackEngine({
    eventStore,
    stateRepository,
    snapshotStore,
    eventIdGenerator: () => `recovery-event-${++eventId}`,
    clock: () => new Date(Date.UTC(2026, 7, 14, 11, 0, timestamp++)).toISOString(),
  });

  return { engine, eventStore, stateRepository };
}

test("recovers a complete live state after the state repository is cleared", () => {
  const { engine, eventStore, stateRepository } = createRecoveryHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
    simulateFailureAt: FAILURE_POINTS.AFTER_PAYMENT,
  });
  const expectedState = engine.replay(checkout.aggregateId);
  const eventCountBeforeReset = eventStore.getAll().length;

  stateRepository.reset();

  assert.equal(engine.getLiveState(checkout.aggregateId), null);
  assert.equal(eventStore.getAll().length, eventCountBeforeReset);

  const recoveredState = engine.recover(checkout.aggregateId, { useSnapshot: false });

  assert.deepEqual(recoveredState, expectedState);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), expectedState);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), engine.replay(checkout.aggregateId));
  assert.equal(eventStore.getAll().length, eventCountBeforeReset);
});

test("recovers from a snapshot and all events appended after it", () => {
  const { engine, eventStore, stateRepository } = createRecoveryHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });

  engine.createSnapshot(checkout.aggregateId);
  engine.compensate(checkout.aggregateId, "Manual rollback");

  const expectedState = engine.replay(checkout.aggregateId);
  const eventCountBeforeReset = eventStore.getAll().length;

  stateRepository.reset();

  const recoveredState = engine.recover(checkout.aggregateId);

  assert.deepEqual(recoveredState, expectedState);
  assert.deepEqual(engine.replayFromSnapshot(checkout.aggregateId), expectedState);
  assert.equal(eventStore.getAll().length, eventCountBeforeReset);
});

test("documents replay as authoritative while a materialized read is unavailable", () => {
  const { engine, eventStore, stateRepository } = createRecoveryHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const expectedState = engine.replay(checkout.aggregateId);
  const eventCount = eventStore.getAll().length;

  stateRepository.reset();

  assert.equal(engine.getLiveState(checkout.aggregateId), null);
  assert.deepEqual(engine.listOrders(), []);
  assert.deepEqual(engine.replay(checkout.aggregateId), expectedState);

  const recoveredState = engine.recover(checkout.aggregateId);

  assert.deepEqual(recoveredState, expectedState);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), expectedState);
  assert.equal(engine.listOrders().length, 1);
  assert.equal(eventStore.getAll().length, eventCount);
});

test("recovery replaces a same-version corrupted materialized view without new events", () => {
  const { engine, eventStore, stateRepository } = createRecoveryHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const expectedState = engine.replay(checkout.aggregateId);
  const eventCount = eventStore.getAll().length;
  const corruptedState = stateRepository.getByAggregateId(checkout.aggregateId);

  corruptedState.order.item = "CORRUPTED";
  stateRepository.replace(corruptedState);

  assert.equal(engine.getLiveState(checkout.aggregateId).order.item, "CORRUPTED");
  assert.equal(engine.replay(checkout.aggregateId).order.item, "Pizza");

  const recoveredState = engine.recover(checkout.aggregateId, { useSnapshot: false });

  assert.deepEqual(recoveredState, expectedState);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), expectedState);
  assert.equal(eventStore.getAll().length, eventCount);
});
