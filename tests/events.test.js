const test = require("node:test");
const assert = require("node:assert/strict");

const {
  EVENT_TYPES,
  assertDomainEvent,
  createDomainEvent,
} = require("../src/domain/events");

const TIMESTAMP = "2026-08-14T10:00:00.000Z";

test("createDomainEvent creates the canonical event structure", () => {
  const event = createDomainEvent({
    eventId: "event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 42,
    sequence: 1,
    timestamp: TIMESTAMP,
    payload: {
      item: "Pizza",
      quantity: 2,
    },
    metadata: {
      schemaVersion: 1,
      commandId: "checkout-command-42",
      correlationId: "checkout-42",
      causationId: "command-42",
    },
  });

  assert.deepEqual(event, {
    eventId: "event-1",
    eventType: "ORDER_CREATED",
    aggregateId: 42,
    sequence: 1,
    timestamp: TIMESTAMP,
    payload: {
      item: "Pizza",
      quantity: 2,
    },
    metadata: {
      schemaVersion: 1,
      commandId: "checkout-command-42",
      correlationId: "checkout-42",
      causationId: "command-42",
    },
  });
  assert.equal(Object.isFrozen(event), true);
  assert.equal(Object.isFrozen(event.payload), true);
  assert.equal(Object.isFrozen(event.metadata), true);
});

test("createDomainEvent generates an event ID and normalized timestamp by default", () => {
  const event = createDomainEvent({
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: "order-1",
    sequence: 1,
    payload: {
      item: "Pizza",
      quantity: 1,
    },
  });

  assert.match(event.eventId, /^[0-9a-f-]{36}$/i);
  assert.equal(new Date(event.timestamp).toISOString(), event.timestamp);
  assert.match(event.metadata.commandId, /^[0-9a-f-]{36}$/i);
  assert.notEqual(event.metadata.commandId, event.eventId);
  assert.deepEqual(event.metadata, {
    schemaVersion: 1,
    commandId: event.metadata.commandId,
    correlationId: event.metadata.commandId,
    causationId: event.metadata.commandId,
  });
});

test("sequences are explicit positive integers and remain ordered by the caller", () => {
  const first = createDomainEvent({
    eventId: "event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: TIMESTAMP,
    payload: { item: "Pizza", quantity: 1 },
  });
  const second = createDomainEvent({
    eventId: "event-2",
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    timestamp: "2026-08-14T10:00:01.000Z",
    payload: { reservationId: 9, item: "Pizza", quantity: 1 },
  });

  assert.equal(first.sequence, 1);
  assert.equal(second.sequence, 2);

  assert.throws(
    () =>
      createDomainEvent({
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 1,
        sequence: 0,
        payload: { item: "Pizza", quantity: 1 },
      }),
    /sequence must be a positive safe integer/
  );
});

test("unsupported event types are rejected", () => {
  assert.throws(
    () =>
      createDomainEvent({
        eventType: "UNKNOWN_EVENT",
        aggregateId: 1,
        sequence: 1,
        payload: {},
      }),
    /Unsupported event type: UNKNOWN_EVENT/
  );
});

test("assertDomainEvent rejects non-normalized timestamps and invalid payloads", () => {
  assert.throws(
    () =>
      assertDomainEvent({
        eventId: "event-1",
        eventType: EVENT_TYPES.PAYMENT_CHARGED,
        aggregateId: 1,
        sequence: 1,
        timestamp: "2026-08-14T12:00:00+02:00",
        payload: { paymentId: 1, amount: 100 },
      }),
    /timestamp must use normalized ISO-8601 UTC format/
  );

  assert.throws(
    () =>
      createDomainEvent({
        eventType: EVENT_TYPES.PAYMENT_CHARGED,
        aggregateId: 1,
        sequence: 1,
        payload: { paymentId: 1, amount: -1 },
      }),
    /payload.amount must be a positive finite number/
  );
});

test("event metadata requires a schema version and trace identifiers", () => {
  assert.throws(
    () =>
      createDomainEvent({
        eventId: "event-invalid-metadata",
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 1,
        sequence: 1,
        timestamp: TIMESTAMP,
        payload: { item: "Pizza", quantity: 1 },
        metadata: {
          schemaVersion: 0,
          commandId: "checkout-command-1",
          correlationId: "checkout-1",
          causationId: "command-1",
        },
      }),
    /metadata.schemaVersion must be a positive safe integer/
  );

  assert.throws(
    () =>
      createDomainEvent({
        eventId: "event-missing-causation",
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 1,
        sequence: 1,
        timestamp: TIMESTAMP,
        payload: { item: "Pizza", quantity: 1 },
        metadata: {
          schemaVersion: 1,
          commandId: "checkout-command-1",
          correlationId: "checkout-1",
        },
      }),
    /metadata.causationId must be a non-empty string or a positive safe integer/
  );

  assert.throws(
    () =>
      createDomainEvent({
        eventId: "event-unsupported-schema",
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 1,
        sequence: 1,
        timestamp: TIMESTAMP,
        payload: { item: "Pizza", quantity: 1 },
        metadata: {
          schemaVersion: 2,
          commandId: "checkout-command-1",
          correlationId: "checkout-1",
          causationId: "command-1",
        },
      }),
    /Unsupported event schema version: 2/
  );

  assert.throws(
    () =>
      createDomainEvent({
        eventId: "self-caused-event",
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 1,
        sequence: 1,
        timestamp: TIMESTAMP,
        payload: { item: "Pizza", quantity: 1 },
        metadata: {
          schemaVersion: 1,
          commandId: "checkout-command-1",
          correlationId: "checkout-1",
          causationId: "self-caused-event",
        },
      }),
    /metadata.causationId cannot reference the event itself/
  );
});
