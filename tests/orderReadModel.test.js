const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { InMemoryStateRepository } = require("../src/infrastructure/inMemoryStateRepository");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");

test("getOrder returns order under materialized and authoritative consistency", () => {
  const engine = new RollbackEngine();
  const checkout = engine.checkout({
    item: "Monitor",
    quantity: 2,
    amount: 500,
  });

  const orderMat = engine.getOrder(checkout.aggregateId, { consistency: "materialized" });
  assert.equal(orderMat.id, checkout.aggregateId);
  assert.equal(orderMat.item, "Monitor");
  assert.equal(orderMat.quantity, 2);

  const orderAuth = engine.getOrder(checkout.aggregateId, { consistency: "authoritative" });
  assert.deepEqual(orderAuth, orderMat);
});

test("getOrder under authoritative consistency self-heals corrupted materialized view", () => {
  const stateRepository = new InMemoryStateRepository();
  const eventStore = new InMemoryEventStore();
  const engine = new RollbackEngine({
    eventStore,
    stateRepository,
  });

  const checkout = engine.checkout({
    item: "Desk",
    quantity: 1,
    amount: 300,
  });

  // Simulate materialized view corruption/loss (e.g. cache eviction or bug)
  const corruptedState = structuredClone(stateRepository.getByAggregateId(checkout.aggregateId));
  corruptedState.order.item = "HACKED_VALUE";
  stateRepository.replace(corruptedState);

  // Materialized read returns corrupted cache
  const readMat = engine.getOrder(checkout.aggregateId, { consistency: "materialized" });
  assert.equal(readMat.item, "HACKED_VALUE");

  // Authoritative read detects drift against Event Store and self-heals
  const readAuth = engine.getOrder(checkout.aggregateId, { consistency: "authoritative" });
  assert.equal(readAuth.item, "Desk");

  // Verify the materialized repository is now repaired
  const repairedMat = stateRepository.getByAggregateId(checkout.aggregateId);
  assert.equal(repairedMat.order.item, "Desk");
});

test("getOrder returns null for non-existent or deleted aggregate", () => {
  const engine = new RollbackEngine();
  assert.equal(engine.getOrder(9999), null);

  const order = engine.createOrder({ item: "Pen", quantity: 5 });
  engine.deleteOrder(order.aggregateId);

  assert.equal(engine.getOrder(order.aggregateId), null);
});

test("getOrder rejects invalid consistency parameter", () => {
  const engine = new RollbackEngine();
  assert.throws(() => engine.getOrder(1, { consistency: "eventual_maybe" }), TypeError);
});
