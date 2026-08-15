const { assertDomainEvent } = require("../domain/events");

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function assertAggregateId(aggregateId) {
  if (!isIdentifier(aggregateId)) {
    throw new TypeError("aggregateId must be a non-empty string or a positive safe integer");
  }
}

function assertExpectedVersion(expectedVersion) {
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 0) {
    throw new TypeError("expectedVersion must be a non-negative safe integer");
  }
}

function createOptimisticConcurrencyError(aggregateId, expectedVersion, actualVersion) {
  const error = new Error(
    `Expected aggregate ${aggregateId} at version ${expectedVersion}, but current version is ${actualVersion}`
  );
  error.code = "OPTIMISTIC_CONCURRENCY_CONFLICT";
  error.aggregateId = aggregateId;
  error.expectedVersion = expectedVersion;
  error.actualVersion = actualVersion;
  return error;
}

function deepFreeze(value, seen = new WeakSet()) {
  if (!value || typeof value !== "object" || seen.has(value)) {
    return value;
  }

  seen.add(value);

  for (const nestedValue of Object.values(value)) {
    deepFreeze(nestedValue, seen);
  }

  return Object.freeze(value);
}

function cloneAndFreeze(value) {
  return deepFreeze(structuredClone(value));
}

class InMemoryEventStore {
  #events = [];

  #eventsByAggregateId = new Map();

  #eventIds = new Set();

  #eventsByCommandId = new Map();

  append(event, { expectedVersion } = {}) {
    assertDomainEvent(event);
    assertExpectedVersion(expectedVersion);

    const aggregateEvents = this.#eventsByAggregateId.get(event.aggregateId) || [];
    const actualVersion = aggregateEvents.length;

    if (actualVersion !== expectedVersion) {
      throw createOptimisticConcurrencyError(
        event.aggregateId,
        expectedVersion,
        actualVersion
      );
    }

    const expectedSequence = expectedVersion + 1;

    if (event.sequence !== expectedSequence) {
      throw new Error(
        `Expected event sequence ${expectedSequence} for aggregate ${event.aggregateId}, received ${event.sequence}`
      );
    }

    const previousEvent = aggregateEvents[aggregateEvents.length - 1];

    if (
      previousEvent &&
      new Date(event.timestamp).getTime() < new Date(previousEvent.timestamp).getTime()
    ) {
      throw new Error(
        `Event timestamp cannot move backwards for aggregate ${event.aggregateId}`
      );
    }

    if (this.#eventIds.has(event.eventId)) {
      throw new Error(`Event ID ${event.eventId} already exists`);
    }

    const storedEvent = cloneAndFreeze(event);

    aggregateEvents.push(storedEvent);
    this.#eventsByAggregateId.set(event.aggregateId, aggregateEvents);
    this.#events.push(storedEvent);
    this.#eventIds.add(storedEvent.eventId);

    const commandEvents = this.#eventsByCommandId.get(storedEvent.metadata.commandId) || [];
    commandEvents.push(storedEvent);
    this.#eventsByCommandId.set(storedEvent.metadata.commandId, commandEvents);

    return storedEvent;
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);

    return [...(this.#eventsByAggregateId.get(aggregateId) || [])];
  }

  getByAggregateIdAfter(aggregateId, sequence) {
    assertAggregateId(aggregateId);

    if (!Number.isSafeInteger(sequence) || sequence < 0) {
      throw new TypeError("sequence must be a non-negative safe integer");
    }

    return this.getByAggregateId(aggregateId).filter((event) => event.sequence > sequence);
  }

  getByCommandId(commandId) {
    if (!isIdentifier(commandId)) {
      throw new TypeError("commandId must be a non-empty string or a positive safe integer");
    }

    return [...(this.#eventsByCommandId.get(commandId) || [])];
  }

  getAll() {
    return [...this.#events];
  }

  getLastSequence(aggregateId) {
    assertAggregateId(aggregateId);

    const aggregateEvents = this.#eventsByAggregateId.get(aggregateId);

    if (!aggregateEvents || aggregateEvents.length === 0) {
      return 0;
    }

    return aggregateEvents[aggregateEvents.length - 1].sequence;
  }
}

module.exports = {
  InMemoryEventStore,
};
