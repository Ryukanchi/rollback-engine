const test = require("node:test");
const assert = require("node:assert/strict");

const { EventUpcasterRegistry } = require("../src/domain/eventUpcaster");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");

test("EventUpcasterRegistry upcasts event across single schema step", () => {
  const registry = new EventUpcasterRegistry();

  registry.register({
    eventType: EVENT_TYPES.ORDER_CREATED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        currency: "USD",
      },
    }),
  });

  const eventV1 = createDomainEvent({
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Book", quantity: 2 },
  });

  assert.equal(eventV1.metadata.schemaVersion, 1);

  const eventV2 = registry.upcast(eventV1);

  assert.equal(eventV2.metadata.schemaVersion, 2);
  assert.equal(eventV2.payload.currency, "USD");
  assert.equal(eventV2.payload.item, "Book");
  assert.equal(eventV2.payload.quantity, 2);
  assert.equal(eventV2.eventId, eventV1.eventId);
});

test("EventUpcasterRegistry upcasts event across multiple chained schema steps", () => {
  const registry = new EventUpcasterRegistry();

  registry.register({
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        currency: "EUR",
      },
    }),
  });

  registry.register({
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    fromVersion: 2,
    toVersion: 3,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        paymentGateway: "stripe_mock",
      },
    }),
  });

  const eventV1 = createDomainEvent({
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    aggregateId: 1,
    sequence: 3,
    payload: { paymentId: 101, amount: 250 },
  });

  const eventV3 = registry.upcast(eventV1);

  assert.equal(eventV3.metadata.schemaVersion, 3);
  assert.equal(eventV3.payload.currency, "EUR");
  assert.equal(eventV3.payload.paymentGateway, "stripe_mock");
  assert.equal(eventV3.payload.amount, 250);
});

test("EventUpcasterRegistry leaves events without registered upcasters unchanged", () => {
  const registry = new EventUpcasterRegistry();
  const event = createDomainEvent({
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    payload: { reservationId: 1, item: "Book", quantity: 2 },
  });

  const unchanged = registry.upcast(event);
  assert.equal(unchanged, event);
});

test("InMemoryEventStore seamlessly upcasts events on read when upcasterRegistry is configured", () => {
  const registry = new EventUpcasterRegistry();
  registry.register({
    eventType: EVENT_TYPES.ORDER_CREATED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        normalizedTag: "v2_migrated",
      },
    }),
  });

  const store = new InMemoryEventStore({ upcasterRegistry: registry });
  const eventV1 = createDomainEvent({
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 42,
    sequence: 1,
    payload: { item: "Laptop", quantity: 1 },
  });

  store.append(eventV1, { expectedVersion: 0 });

  const readEvents = store.getByAggregateId(42);
  assert.equal(readEvents.length, 1);
  assert.equal(readEvents[0].metadata.schemaVersion, 2);
  assert.equal(readEvents[0].payload.normalizedTag, "v2_migrated");
});

test("EventUpcasterRegistry rejects invalid registration inputs", () => {
  const registry = new EventUpcasterRegistry();

  assert.throws(
    () => registry.register({ eventType: "", fromVersion: 1, toVersion: 2, upcast: () => {} }),
    TypeError
  );

  assert.throws(
    () => registry.register({ eventType: "ORDER_CREATED", fromVersion: 2, toVersion: 1, upcast: () => {} }),
    /toVersion must be strictly greater/
  );

  assert.throws(
    () => registry.register({ eventType: "ORDER_CREATED", fromVersion: 1, toVersion: 2, upcast: null }),
    TypeError
  );
});
