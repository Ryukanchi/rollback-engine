const test = require("node:test");
const assert = require("node:assert/strict");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { validateCheckoutCommand } = require("../src/domain/checkoutSaga");
const { InvariantSuite } = require("../src/chaos/invariantSuite");

test("checkout with simulateFailureAt 'after_order' executes 2 events (ORDER_CREATED, ORDER_ROLLED_BACK)", () => {
  const adapters = createStorageAdapters({ type: "memory" });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
  });

  const result = engine.checkout({
    item: "QuantumSensor",
    quantity: 2,
    amount: 500,
    simulateFailureAt: "after_order",
  });

  assert.equal(result.status, "rolled_back");
  assert.equal(result.events.length, 2);
  assert.equal(result.events[0].eventType, "ORDER_CREATED");
  assert.equal(result.events[1].eventType, "ORDER_ROLLED_BACK");

  const state = engine.replay(result.aggregateId);
  assert.equal(state.lifecycle, "rolled_back");
  assert.equal(state.order.status, "rolled_back");
  assert.equal(state.inventory, null);
  assert.equal(state.payment, null);

  const suite = new InvariantSuite();
  suite.checkAll({ engine, adapters, aggregateIds: [result.aggregateId] });
});

test("checkout with simulateFailureAt 'after_inventory' executes 4 events", () => {
  const adapters = createStorageAdapters({ type: "memory" });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
  });

  const result = engine.checkout({
    item: "RoboticArm",
    quantity: 1,
    amount: 1500,
    simulateFailureAt: "after_inventory",
  });

  assert.equal(result.status, "rolled_back");
  assert.equal(result.events.length, 4);
  assert.equal(result.events[0].eventType, "ORDER_CREATED");
  assert.equal(result.events[1].eventType, "INVENTORY_RESERVED");
  assert.equal(result.events[2].eventType, "INVENTORY_RELEASED");
  assert.equal(result.events[3].eventType, "ORDER_ROLLED_BACK");

  const state = engine.replay(result.aggregateId);
  assert.equal(state.lifecycle, "rolled_back");
  assert.equal(state.inventory.status, "released");
  assert.equal(state.order.status, "rolled_back");
  assert.equal(state.payment, null);

  const suite = new InvariantSuite();
  suite.checkAll({ engine, adapters, aggregateIds: [result.aggregateId] });
});

test("checkout with simulateFailureAt 'after_payment' executes 6 events", () => {
  const adapters = createStorageAdapters({ type: "memory" });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
  });

  const result = engine.checkout({
    item: "HighEndGPU",
    quantity: 4,
    amount: 3200,
    simulateFailureAt: "after_payment",
  });

  assert.equal(result.status, "rolled_back");
  assert.equal(result.events.length, 6);
  assert.equal(result.events[0].eventType, "ORDER_CREATED");
  assert.equal(result.events[1].eventType, "INVENTORY_RESERVED");
  assert.equal(result.events[2].eventType, "PAYMENT_CHARGED");
  assert.equal(result.events[3].eventType, "PAYMENT_REFUNDED");
  assert.equal(result.events[4].eventType, "INVENTORY_RELEASED");
  assert.equal(result.events[5].eventType, "ORDER_ROLLED_BACK");

  const state = engine.replay(result.aggregateId);
  assert.equal(state.lifecycle, "rolled_back");
  assert.equal(state.payment.status, "refunded");
  assert.equal(state.inventory.status, "released");
  assert.equal(state.order.status, "rolled_back");

  const suite = new InvariantSuite();
  suite.checkAll({ engine, adapters, aggregateIds: [result.aggregateId] });
});

test("checkout validation rejects invalid simulateFailureAt strings", () => {
  assert.throws(
    () => {
      validateCheckoutCommand({
        item: "ServerRack",
        quantity: 1,
        amount: 2000,
        simulateFailureAt: "invalid_failure_point",
      });
    },
    {
      name: "TypeError",
      message: /simulateFailureAt must be one of: after_order, after_inventory, after_payment/,
    }
  );
});
