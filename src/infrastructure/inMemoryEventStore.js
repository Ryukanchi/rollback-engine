const { assertDomainEvent } = require("../domain/events");
const {
  createFencingTokenStaleError,
  createFencingTokenRequiredError,
  createCommandLeaseExpiredError,
} = require("../application/errors");

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

  #subscribers = new Set();

  #upcasterRegistry;

  #commandStore;

  #now;

  constructor({ upcasterRegistry, commandStore = null, now = () => Date.now() } = {}) {
    this.#upcasterRegistry = upcasterRegistry;
    this.#commandStore = commandStore;
    this.#now = typeof now === "function" ? now : () => Date.now();
  }

  setCommandStore(commandStore) {
    this.#commandStore = commandStore;
  }

  setNow(now) {
    this.#now = typeof now === "function" ? now : () => Date.now();
  }

  append(event, { expectedVersion, fencingToken } = {}) {
    assertDomainEvent(event);
    assertExpectedVersion(expectedVersion);

    if (this.#commandStore && event.metadata?.commandId) {
      const cmd = this.#commandStore.get(event.metadata.commandId);
      if (cmd) {
        if (cmd.status !== "processing") {
          throw createFencingTokenStaleError({
            commandId: event.metadata.commandId,
            providedToken: fencingToken,
            currentToken: cmd.leaseToken,
            leaseOwner: cmd.leaseOwner,
          });
        }

        if (cmd.leaseToken !== null && cmd.leaseToken !== undefined) {
          const currentToken = Number(cmd.leaseToken);
          if (fencingToken === undefined || fencingToken === null) {
            throw createFencingTokenRequiredError({
              commandId: event.metadata.commandId,
              leaseOwner: cmd.leaseOwner,
            });
          }

          if (Number(fencingToken) !== currentToken) {
            throw createFencingTokenStaleError({
              commandId: event.metadata.commandId,
              providedToken: Number(fencingToken),
              currentToken,
              leaseOwner: cmd.leaseOwner,
            });
          }

          if (cmd.leaseExpiresAt !== null && cmd.leaseExpiresAt !== undefined) {
            const nowMs = this.#now();
            if (Number(cmd.leaseExpiresAt) < nowMs) {
              throw createCommandLeaseExpiredError({
                commandId: event.metadata.commandId,
                fencingToken: Number(fencingToken),
                leaseExpiresAt: Number(cmd.leaseExpiresAt),
                now: nowMs,
              });
            }
          }
        }
      }
    }

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

    this.#notifySubscribers(storedEvent);

    return storedEvent;
  }

  #transform(event) {
    if (!this.#upcasterRegistry || !event) {
      return event;
    }
    return this.#upcasterRegistry.upcast(event);
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);

    const events = this.#eventsByAggregateId.get(aggregateId) || [];
    return events.map((event) => this.#transform(event));
  }

  getByAggregateIdAfter(aggregateId, sequence) {
    assertAggregateId(aggregateId);

    if (!Number.isSafeInteger(sequence) || sequence < 0) {
      throw new TypeError("sequence must be a non-negative safe integer");
    }

    const events = this.#eventsByAggregateId.get(aggregateId) || [];
    const filtered = sequence < events.length
      ? events.slice(sequence)
      : events.filter((event) => event.sequence > sequence);

    return filtered.map((event) => this.#transform(event));
  }

  getByCommandId(commandId) {
    if (!isIdentifier(commandId)) {
      throw new TypeError("commandId must be a non-empty string or a positive safe integer");
    }

    const events = this.#eventsByCommandId.get(commandId) || [];
    return events.map((event) => this.#transform(event));
  }

  getAll() {
    return this.#events.map((event) => this.#transform(event));
  }

  getLastSequence(aggregateId) {
    assertAggregateId(aggregateId);

    const aggregateEvents = this.#eventsByAggregateId.get(aggregateId);

    if (!aggregateEvents || aggregateEvents.length === 0) {
      return 0;
    }

    return aggregateEvents[aggregateEvents.length - 1].sequence;
  }

  subscribe(filterOrHandler, maybeHandler) {
    let filter = {};
    let handler = filterOrHandler;

    if (typeof filterOrHandler === "object" && filterOrHandler !== null) {
      filter = filterOrHandler;
      handler = maybeHandler;
    }

    if (typeof handler !== "function") {
      throw new TypeError("subscription handler must be a function");
    }

    const subscription = { filter, handler };
    this.#subscribers.add(subscription);

    return () => {
      this.#subscribers.delete(subscription);
    };
  }

  #notifySubscribers(event) {
    for (const subscription of this.#subscribers) {
      const { filter, handler } = subscription;

      if (filter.aggregateId !== undefined && filter.aggregateId !== event.aggregateId) {
        continue;
      }

      if (filter.eventType !== undefined && filter.eventType !== event.eventType) {
        continue;
      }

      try {
        const result = handler(event);
        if (result && typeof result.catch === "function") {
          result.catch(() => {});
        }
      } catch {
        // Subscribers are asynchronous / side-effects with error isolation.
      }
    }
  }
}

module.exports = {
  InMemoryEventStore,
};
