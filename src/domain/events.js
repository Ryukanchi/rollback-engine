const { randomUUID } = require("node:crypto");

const EVENT_TYPES = Object.freeze({
  ORDER_CREATED: "ORDER_CREATED",
  INVENTORY_RESERVED: "INVENTORY_RESERVED",
  PAYMENT_CHARGED: "PAYMENT_CHARGED",
  PAYMENT_REFUNDED: "PAYMENT_REFUNDED",
  INVENTORY_RELEASED: "INVENTORY_RELEASED",
  ORDER_ROLLED_BACK: "ORDER_ROLLED_BACK",
  ORDER_DELETED: "ORDER_DELETED",
});

const CURRENT_EVENT_SCHEMA_VERSION = 1;

const SUPPORTED_EVENT_TYPES = new Set(Object.values(EVENT_TYPES));

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function assertIdentifier(value, fieldName) {
  if (!isIdentifier(value)) {
    throw new TypeError(`${fieldName} must be a non-empty string or a positive safe integer`);
  }
}

function assertNonEmptyString(value, fieldName) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
}

function assertPositiveInteger(value, fieldName) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${fieldName} must be a positive safe integer`);
  }
}

function assertPositiveNumber(value, fieldName) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new TypeError(`${fieldName} must be a positive finite number`);
  }
}

function assertOptionalReason(payload) {
  if (payload.reason !== undefined) {
    assertNonEmptyString(payload.reason, "payload.reason");
  }
}

function assertPayload(eventType, payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new TypeError("payload must be an object");
  }

  switch (eventType) {
    case EVENT_TYPES.ORDER_CREATED:
      assertNonEmptyString(payload.item, "payload.item");
      assertPositiveInteger(payload.quantity, "payload.quantity");
      break;

    case EVENT_TYPES.INVENTORY_RESERVED:
      assertIdentifier(payload.reservationId, "payload.reservationId");
      assertNonEmptyString(payload.item, "payload.item");
      assertPositiveInteger(payload.quantity, "payload.quantity");
      break;

    case EVENT_TYPES.PAYMENT_CHARGED:
      assertIdentifier(payload.paymentId, "payload.paymentId");
      assertPositiveNumber(payload.amount, "payload.amount");
      break;

    case EVENT_TYPES.PAYMENT_REFUNDED:
      assertIdentifier(payload.paymentId, "payload.paymentId");
      assertOptionalReason(payload);
      break;

    case EVENT_TYPES.INVENTORY_RELEASED:
      assertIdentifier(payload.reservationId, "payload.reservationId");
      assertOptionalReason(payload);
      break;

    case EVENT_TYPES.ORDER_ROLLED_BACK:
    case EVENT_TYPES.ORDER_DELETED:
      assertOptionalReason(payload);
      break;

    default:
      throw new TypeError(`Unsupported event type: ${eventType}`);
  }
}

function assertMetadata(metadata) {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    throw new TypeError("metadata must be an object");
  }

  assertPositiveInteger(metadata.schemaVersion, "metadata.schemaVersion");

  if (metadata.schemaVersion !== CURRENT_EVENT_SCHEMA_VERSION) {
    throw new TypeError(`Unsupported event schema version: ${metadata.schemaVersion}`);
  }

  assertIdentifier(metadata.commandId, "metadata.commandId");
  assertIdentifier(metadata.correlationId, "metadata.correlationId");
  assertIdentifier(metadata.causationId, "metadata.causationId");
}

function normalizeTimestamp(timestamp) {
  if (typeof timestamp !== "string" || timestamp.length === 0) {
    throw new TypeError("timestamp must be a valid date string");
  }

  const parsedTimestamp = new Date(timestamp);

  if (Number.isNaN(parsedTimestamp.getTime())) {
    throw new TypeError("timestamp must be a valid date string");
  }

  return parsedTimestamp.toISOString();
}

function assertDomainFact(event) {
  if (!event || typeof event !== "object" || Array.isArray(event)) {
    throw new TypeError("event must be an object");
  }

  if (!SUPPORTED_EVENT_TYPES.has(event.eventType)) {
    throw new TypeError(`Unsupported event type: ${event.eventType}`);
  }

  assertIdentifier(event.aggregateId, "aggregateId");
  assertPositiveInteger(event.sequence, "sequence");

  const normalizedTimestamp = normalizeTimestamp(event.timestamp);

  if (normalizedTimestamp !== event.timestamp) {
    throw new TypeError("timestamp must use normalized ISO-8601 UTC format");
  }

  assertPayload(event.eventType, event.payload);

  return event;
}

function assertDomainEvent(event) {
  assertDomainFact(event);
  assertNonEmptyString(event.eventId, "eventId");
  assertMetadata(event.metadata);

  if (event.metadata.causationId === event.eventId) {
    throw new TypeError("metadata.causationId cannot reference the event itself");
  }

  return event;
}

function createDomainEvent({
  eventId = randomUUID(),
  eventType,
  aggregateId,
  sequence,
  timestamp = new Date().toISOString(),
  payload = {},
  metadata,
} = {}) {
  let resolvedMetadata = metadata;

  if (resolvedMetadata === undefined) {
    const generatedCommandId = randomUUID();
    resolvedMetadata = {
      schemaVersion: CURRENT_EVENT_SCHEMA_VERSION,
      commandId: generatedCommandId,
      correlationId: generatedCommandId,
      causationId: generatedCommandId,
    };
  }

  const event = {
    eventId,
    eventType,
    aggregateId,
    sequence,
    timestamp: normalizeTimestamp(timestamp),
    payload: Object.freeze({ ...payload }),
    metadata: Object.freeze({ ...resolvedMetadata }),
  };

  assertDomainEvent(event);

  return Object.freeze(event);
}

module.exports = {
  CURRENT_EVENT_SCHEMA_VERSION,
  EVENT_TYPES,
  assertDomainFact,
  assertDomainEvent,
  createDomainEvent,
};
