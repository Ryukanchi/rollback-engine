const test = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");

function event({
  eventId,
  eventType,
  aggregateId,
  sequence,
  payload,
  metadata,
}) {
  return createDomainEvent({
    eventId,
    eventType,
    aggregateId,
    sequence,
    timestamp: `2026-08-14T10:00:0${sequence}.000Z`,
    payload,
    metadata,
  });
}

function append(store, domainEvent, expectedVersion = domainEvent.sequence - 1) {
  return store.append(domainEvent, { expectedVersion });
}

test("appends valid events and reads them in aggregate sequence", () => {
  const store = new InMemoryEventStore();
  const created = event({
    eventId: "order-1-created",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Pizza", quantity: 1 },
  });
  const reserved = event({
    eventId: "order-1-reserved",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    payload: { reservationId: 10, item: "Pizza", quantity: 1 },
  });

  assert.deepEqual(append(store, created), created);
  assert.deepEqual(append(store, reserved), reserved);
  assert.deepEqual(store.getByAggregateId(1), [created, reserved]);
  assert.deepEqual(store.getByAggregateIdAfter(1, 0), [created, reserved]);
  assert.deepEqual(store.getByAggregateIdAfter(1, 1), [reserved]);
  assert.deepEqual(store.getByAggregateIdAfter(1, 2), []);
  assert.deepEqual(store.getByAggregateIdAfter(1, 100), []);
  assert.deepEqual(store.getAll(), [created, reserved]);
  assert.equal(store.getLastSequence(1), 2);
  assert.equal(store.getLastSequence(999), 0);
});

test("rejects invalid events and non-contiguous aggregate sequences", () => {
  const store = new InMemoryEventStore();
  const sequenceTwo = event({
    eventId: "order-1-reserved",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    payload: { reservationId: 10, item: "Pizza", quantity: 1 },
  });

  assert.throws(() => append(store, sequenceTwo, 0), /Expected event sequence 1/);

  const created = event({
    eventId: "order-1-created",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Pizza", quantity: 1 },
  });

  append(store, created);

  assert.throws(() => append(store, created, 1), /Expected event sequence 2/);
  assert.throws(
    () =>
      append(
        store,
        {
          eventId: "invalid",
          eventType: "UNKNOWN_EVENT",
          aggregateId: 1,
          sequence: 2,
          timestamp: "2026-08-14T10:00:02.000Z",
          payload: {},
        },
        1
      ),
    /Unsupported event type/
  );
  assert.equal(store.getAll().length, 1);
});

test("keeps aggregates separate even when their internal entity IDs match", () => {
  const store = new InMemoryEventStore();
  const firstAggregateEvent = event({
    eventId: "aggregate-1-reservation",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 1,
    payload: { reservationId: 50, item: "Pizza", quantity: 1 },
  });
  const secondAggregateEvent = event({
    eventId: "aggregate-2-reservation",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 2,
    sequence: 1,
    payload: { reservationId: 50, item: "Pasta", quantity: 2 },
  });

  append(store, firstAggregateEvent);
  append(store, secondAggregateEvent);

  assert.deepEqual(store.getByAggregateId(1), [firstAggregateEvent]);
  assert.deepEqual(store.getByAggregateId(2), [secondAggregateEvent]);
  assert.equal(store.getLastSequence(1), 1);
  assert.equal(store.getLastSequence(2), 1);
  assert.equal(store.getAll().length, 2);
});

test("stored events remain append-only when callers mutate inputs or returned arrays", () => {
  const store = new InMemoryEventStore();
  const mutableEvent = {
    eventId: "mutable-event",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: "2026-08-14T10:00:01.000Z",
    payload: { item: "Pizza", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId: "mutable-command",
      correlationId: "mutable-command",
      causationId: "mutable-command",
    },
  };

  const storedEvent = append(store, mutableEvent);
  mutableEvent.payload.item = "Changed outside the store";
  mutableEvent.metadata.correlationId = "changed-outside-the-store";

  const returnedEvents = store.getAll();
  returnedEvents.length = 0;

  assert.equal(storedEvent.payload.item, "Pizza");
  assert.equal(Object.isFrozen(storedEvent), true);
  assert.equal(Object.isFrozen(storedEvent.payload), true);
  assert.equal(Object.isFrozen(storedEvent.metadata), true);
  assert.equal(store.getAll().length, 1);
  assert.equal(store.getAll()[0].payload.item, "Pizza");
  assert.equal(store.getAll()[0].metadata.correlationId, "mutable-command");
});

test("reads committed events by their technical command ID", () => {
  const store = new InMemoryEventStore();
  const metadata = {
    schemaVersion: 1,
    commandId: "checkout-command-1",
    correlationId: "checkout-flow-1",
    causationId: "checkout-command-1",
  };
  const created = event({
    eventId: "command-event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Pizza", quantity: 1 },
    metadata,
  });

  append(store, created);

  assert.deepEqual(store.getByCommandId("checkout-command-1"), [created]);
  assert.deepEqual(store.getByCommandId("missing-command"), []);
});

test("rejects duplicate event IDs across the complete store", () => {
  const store = new InMemoryEventStore();

  append(
    store,
    event({
      eventId: "duplicate-event-id",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 1,
      sequence: 1,
      payload: { item: "Pizza", quantity: 1 },
    })
  );

  assert.throws(
    () =>
      append(
        store,
        event({
          eventId: "duplicate-event-id",
          eventType: EVENT_TYPES.ORDER_CREATED,
          aggregateId: 2,
          sequence: 1,
          payload: { item: "Pasta", quantity: 1 },
        })
      ),
    /Event ID duplicate-event-id already exists/
  );
});

test("accepts equal timestamps but rejects decreasing timestamps within an aggregate", () => {
  const store = new InMemoryEventStore();
  const first = createDomainEvent({
    eventId: "event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: "2026-08-14T10:00:01.000Z",
    payload: { item: "Pizza", quantity: 1 },
  });
  const equalTimestamp = createDomainEvent({
    eventId: "event-2",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    timestamp: "2026-08-14T10:00:01.000Z",
    payload: { reservationId: 10, item: "Pizza", quantity: 1 },
  });
  const decreasingTimestamp = createDomainEvent({
    eventId: "event-3",
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    aggregateId: 1,
    sequence: 3,
    timestamp: "2026-08-14T10:00:00.000Z",
    payload: { paymentId: 20, amount: 100 },
  });

  append(store, first);
  append(store, equalTimestamp);

  assert.throws(
    () => append(store, decreasingTimestamp),
    /Event timestamp cannot move backwards/
  );
  assert.equal(store.getLastSequence(1), 2);
});

test("rejects a stale writer by expected aggregate version without mutating the stream", () => {
  const store = new InMemoryEventStore();
  const created = event({
    eventId: "event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Pizza", quantity: 1 },
  });
  const reservedByStaleWriter = event({
    eventId: "event-2",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    payload: { reservationId: 10, item: "Pizza", quantity: 1 },
  });

  append(store, created, 0);

  assert.throws(
    () => append(store, reservedByStaleWriter, 0),
    (error) => {
      assert.equal(error.code, "OPTIMISTIC_CONCURRENCY_CONFLICT");
      assert.equal(error.aggregateId, 1);
      assert.equal(error.expectedVersion, 0);
      assert.equal(error.actualVersion, 1);
      return true;
    }
  );
  assert.deepEqual(store.getByAggregateId(1), [created]);

  assert.deepEqual(append(store, reservedByStaleWriter, 1), reservedByStaleWriter);
  assert.equal(store.getLastSequence(1), 2);
});

test("requires callers to declare the aggregate version they observed", () => {
  const store = new InMemoryEventStore();
  const created = event({
    eventId: "event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Pizza", quantity: 1 },
  });

  assert.throws(
    () => store.append(created),
    /expectedVersion must be a non-negative safe integer/
  );
  assert.deepEqual(store.getAll(), []);
});
