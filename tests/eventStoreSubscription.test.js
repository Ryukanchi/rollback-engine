const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const { EVENT_TYPES } = require("../src/domain/events");

test("subscribe receives emitted events and can filter by aggregateId and eventType", () => {
  const eventStore = new InMemoryEventStore();
  const engine = new RollbackEngine({ eventStore });

  const allEvents = [];
  const aggregate1Events = [];
  const paymentEvents = [];

  const unsubAll = engine.subscribe({}, (event) => {
    allEvents.push(event);
  });

  const unsubAgg1 = engine.subscribe({ aggregateId: 1 }, (event) => {
    aggregate1Events.push(event);
  });

  const unsubPayments = engine.subscribe({ eventType: EVENT_TYPES.PAYMENT_CHARGED }, (event) => {
    paymentEvents.push(event);
  });

  engine.checkout({ item: "A", quantity: 1, amount: 10 });
  engine.checkout({ item: "B", quantity: 1, amount: 20 });

  assert.equal(allEvents.length, 6); // 3 events per checkout
  assert.equal(aggregate1Events.length, 3);
  assert.equal(paymentEvents.length, 2);
  assert.equal(paymentEvents[0].eventType, EVENT_TYPES.PAYMENT_CHARGED);
  assert.equal(paymentEvents[1].eventType, EVENT_TYPES.PAYMENT_CHARGED);

  // Test unsubscribe
  unsubPayments();
  engine.checkout({ item: "C", quantity: 1, amount: 30 });

  assert.equal(allEvents.length, 9);
  assert.equal(paymentEvents.length, 2); // Unsubscribed, no longer increments

  unsubAll();
  unsubAgg1();
});

test("subscriber exceptions are isolated and do not fail event store appends", () => {
  const eventStore = new InMemoryEventStore();
  const engine = new RollbackEngine({ eventStore });

  let errorSubscriberCalled = false;

  engine.subscribe({}, () => {
    errorSubscriberCalled = true;
    throw new Error("Catastrophic subscriber explosion!");
  });

  const checkout = engine.checkout({ item: "Shield", quantity: 1, amount: 99 });

  assert.equal(errorSubscriberCalled, true);
  assert.equal(checkout.status, "completed");
  assert.equal(engine.getEvents(checkout.aggregateId).length, 3);
});
