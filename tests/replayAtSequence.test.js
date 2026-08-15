const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");

test("replayAtSequence reconstructs intermediate states regardless of equal timestamps", () => {
  const eventStore = new InMemoryEventStore();
  const fixedTimestamp = "2026-08-15T12:00:00.000Z";
  const engine = new RollbackEngine({
    eventStore,
    clock: () => fixedTimestamp,
  });

  const checkout = engine.checkout({
    item: "Keyboard",
    quantity: 1,
    amount: 150,
  });

  const aggregateId = checkout.aggregateId;

  // All 3 events share the exact same timestamp!
  const events = engine.getEvents(aggregateId);
  assert.equal(events.length, 3);
  assert.equal(events[0].timestamp, fixedTimestamp);
  assert.equal(events[1].timestamp, fixedTimestamp);
  assert.equal(events[2].timestamp, fixedTimestamp);

  // Timestamp-based replay cannot isolate step 1 or step 2 because they have the same timestamp
  const stateAtTime = engine.replayAt(aggregateId, fixedTimestamp);
  assert.equal(stateAtTime.version, 3);

  // Exact sequence-based replay can deterministically reconstruct every point in time
  const state0 = engine.replayAtSequence(aggregateId, 0);
  assert.equal(state0.version, 0);
  assert.equal(state0.lifecycle, "empty");
  assert.equal(state0.order, null);

  const state1 = engine.replayAtSequence(aggregateId, 1);
  assert.equal(state1.version, 1);
  assert.equal(state1.lifecycle, "active");
  assert.equal(state1.order.status, "created");
  assert.equal(state1.inventory, null);
  assert.equal(state1.payment, null);

  const state2 = engine.replayAtSequence(aggregateId, 2);
  assert.equal(state2.version, 2);
  assert.equal(state2.lifecycle, "active");
  assert.equal(state2.order.status, "created");
  assert.equal(state2.inventory.status, "reserved");
  assert.equal(state2.payment, null);

  const state3 = engine.replayAtSequence(aggregateId, 3);
  assert.equal(state3.version, 3);
  assert.equal(state3.lifecycle, "completed");
  assert.equal(state3.order.status, "created");
  assert.equal(state3.inventory.status, "reserved");
  assert.equal(state3.payment.status, "charged");
});

test("replayAtSequence returns null for non-existent aggregate", () => {
  const engine = new RollbackEngine();
  assert.equal(engine.replayAtSequence(99999, 1), null);
});

test("replayAtSequence rejects negative or non-integer sequence numbers", () => {
  const engine = new RollbackEngine();
  assert.throws(() => engine.replayAtSequence(1, -1), TypeError);
  assert.throws(() => engine.replayAtSequence(1, 1.5), TypeError);
  assert.throws(() => engine.replayAtSequence(1, "1"), TypeError);
});
