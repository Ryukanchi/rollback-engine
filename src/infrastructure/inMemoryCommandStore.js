const { isDeepStrictEqual } = require("node:util");
const { COMMAND_STATUSES } = require("../application/storeContracts");
const {
  createFencingTokenStaleError,
} = require("../application/errors");

function assertNonEmptyString(value, fieldName) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
}

function assertPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new TypeError("payload must be an object");
  }
}

function clone(value) {
  return value === null || value === undefined ? value : structuredClone(value);
}

function buildEventRange(events, commandId) {
  if (!Array.isArray(events) || events.length === 0) {
    throw new TypeError("events must be a non-empty array");
  }

  const normalizedEvents = clone(events);
  const [firstEvent] = normalizedEvents;

  for (const [index, event] of normalizedEvents.entries()) {
    if (
      !event ||
      typeof event !== "object" ||
      typeof event.eventId !== "string" ||
      !Number.isSafeInteger(event.sequence) ||
      event.sequence <= 0
    ) {
      throw new TypeError("event must contain an eventId and positive sequence");
    }

    if (event.metadata?.commandId !== commandId) {
      throw new Error(`Event ${event.eventId} does not belong to command ${commandId}`);
    }

    if (event.aggregateId !== firstEvent.aggregateId) {
      throw new Error("Command events cannot span multiple aggregates");
    }

    if (event.sequence !== firstEvent.sequence + index) {
      throw new Error("Command events must be contiguous");
    }
  }

  const lastEvent = normalizedEvents[normalizedEvents.length - 1];

  return {
    aggregateId: firstEvent.aggregateId,
    firstSequence: firstEvent.sequence,
    lastSequence: lastEvent.sequence,
    eventIds: normalizedEvents.map((event) => event.eventId),
  };
}

class InMemoryCommandStore {
  #commands = new Map();
  #eventStore = null;

  setEventStore(eventStore) {
    this.#eventStore = eventStore;
  }

  reserve({
    commandId,
    commandType,
    payload,
    workerId = null,
    leaseTtlMs = 5000,
    now = Date.now(),
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(commandType, "commandType");
    assertPayload(payload);

    const existing = this.#commands.get(commandId);

    if (existing) {
      // Re-reservation of a released command: increment token to prevent ABA
      if (existing.status === COMMAND_STATUSES.RELEASED) {
        const conflict =
          existing.commandType !== commandType ||
          !isDeepStrictEqual(existing.payload, payload);

        if (conflict) {
          return { created: false, conflict: true, record: clone(existing) };
        }

        const leaseOwner = workerId;
        const leaseToken = (existing.leaseToken || 1) + 1;
        const leaseExpiresAt = workerId ? now + leaseTtlMs : null;

        existing.commandType = commandType;
        existing.payload = clone(payload);
        existing.status = COMMAND_STATUSES.PROCESSING;
        existing.aggregateId = null;
        existing.eventRange = null;
        existing.result = null;
        existing.error = null;
        existing.leaseOwner = leaseOwner;
        existing.leaseToken = leaseToken;
        existing.leaseExpiresAt = leaseExpiresAt;

        return { created: true, conflict: false, record: clone(existing) };
      }

      const conflict =
        existing.commandType !== commandType ||
        !isDeepStrictEqual(existing.payload, payload);

      return {
        created: false,
        conflict,
        record: clone(existing),
      };
    }

    const leaseOwner = workerId;
    const leaseToken = 1;
    const leaseExpiresAt = workerId ? now + leaseTtlMs : null;

    const record = {
      commandId,
      commandType,
      payload: clone(payload),
      status: COMMAND_STATUSES.PROCESSING,
      aggregateId: null,
      eventRange: null,
      result: null,
      error: null,
      leaseOwner,
      leaseToken,
      leaseExpiresAt,
    };

    this.#commands.set(commandId, record);

    return {
      created: true,
      conflict: false,
      record: clone(record),
    };
  }

  takeOverExpired({
    commandId,
    workerId,
    leaseTtlMs = 5000,
    now = Date.now(),
    expectedToken,
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(workerId, "workerId");

    const record = this.#commands.get(commandId);

    if (!record) {
      return { success: false, reason: "NOT_FOUND" };
    }

    if (record.status !== COMMAND_STATUSES.PROCESSING) {
      return { success: false, reason: "NOT_PROCESSING" };
    }

    // Authoritative Event Check: check event store directly
    if (this.#eventStore) {
      const events = this.#eventStore.getByCommandId(commandId);
      if (events && events.length > 0) {
        return { success: false, reason: "HAS_EVENTS" };
      }
    }

    if (record.eventRange && (record.eventRange.count > 0 || (Array.isArray(record.eventRange.eventIds) && record.eventRange.eventIds.length > 0))) {
      return { success: false, reason: "HAS_EVENTS" };
    }

    if (record.leaseExpiresAt !== null && record.leaseExpiresAt > now) {
      return { success: false, reason: "NOT_EXPIRED" };
    }

    if (expectedToken !== undefined && record.leaseToken !== expectedToken) {
      return { success: false, reason: "TOKEN_MISMATCH" };
    }

    record.leaseOwner = workerId;
    record.leaseToken = (record.leaseToken || 1) + 1;
    record.leaseExpiresAt = now + leaseTtlMs;

    return {
      success: true,
      record: clone(record),
    };
  }

  renewLease({
    commandId,
    workerId,
    fencingToken,
    leaseTtlMs = 5000,
    now = Date.now(),
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(workerId, "workerId");

    const record = this.#commands.get(commandId);

    if (!record || record.status !== COMMAND_STATUSES.PROCESSING) {
      throw new Error(`Command ${commandId} is not processing`);
    }

    if (record.leaseOwner !== workerId || (fencingToken !== undefined && record.leaseToken !== fencingToken)) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        workerId,
        leaseOwner: record.leaseOwner,
      });
    }

    record.leaseExpiresAt = now + leaseTtlMs;
    return { renewed: true, leaseExpiresAt: record.leaseExpiresAt };
  }

  recordEvent(commandId, event, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    if (
      !event ||
      typeof event !== "object" ||
      typeof event.eventId !== "string" ||
      !Number.isSafeInteger(event.sequence) ||
      event.sequence <= 0
    ) {
      throw new TypeError("event must contain an eventId and positive sequence");
    }

    if (event.metadata?.commandId !== commandId) {
      throw new Error(`Event ${event.eventId} does not belong to command ${commandId}`);
    }

    if (!record.eventRange) {
      record.aggregateId = event.aggregateId;
      record.eventRange = {
        aggregateId: event.aggregateId,
        firstSequence: event.sequence,
        lastSequence: event.sequence,
        eventIds: [event.eventId],
      };
      return clone(record);
    }

    if (record.eventRange.aggregateId !== event.aggregateId) {
      throw new Error(`Command ${commandId} cannot span multiple aggregates`);
    }

    if (event.sequence !== record.eventRange.lastSequence + 1) {
      throw new Error(`Command ${commandId} events must be contiguous`);
    }

    record.eventRange.lastSequence = event.sequence;
    record.eventRange.eventIds.push(event.eventId);

    return clone(record);
  }

  complete(commandId, result, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    const clonedResult = clone(result);

    record.status = COMMAND_STATUSES.COMPLETED;
    record.result = clonedResult;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    return clone(record);
  }

  fail(commandId, error, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    if (!error || typeof error !== "object" || Array.isArray(error)) {
      throw new TypeError("error must be an object");
    }

    const clonedError = clone(error);

    record.status = COMMAND_STATUSES.FAILED;
    record.error = clonedError;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    return clone(record);
  }

  release(commandId, { fencingToken } = {}) {
    assertNonEmptyString(commandId, "commandId");

    const record = this.#commands.get(commandId);

    if (!record) {
      return false;
    }

    if (record.status !== COMMAND_STATUSES.PROCESSING || record.eventRange) {
      throw new Error(`Command ${commandId} cannot be released`);
    }

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    record.status = COMMAND_STATUSES.RELEASED;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    return true;
  }

  releaseFailed(commandId, expectedErrorCode, { fencingToken } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(expectedErrorCode, "expectedErrorCode");

    const record = this.#commands.get(commandId);

    if (!record) {
      return false;
    }

    if (
      record.status !== COMMAND_STATUSES.FAILED ||
      record.eventRange ||
      record.error?.code !== expectedErrorCode
    ) {
      throw new Error(`Failed command ${commandId} cannot be released`);
    }

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    return this.#commands.delete(commandId);
  }

  get(commandId) {
    assertNonEmptyString(commandId, "commandId");

    return clone(this.#commands.get(commandId) ?? null);
  }

  reconcileEvents(commandId, events) {
    const record = this.#requireProcessing(commandId);
    const eventRange = buildEventRange(events, commandId);

    record.aggregateId = eventRange.aggregateId;
    record.eventRange = eventRange;

    return clone(record);
  }

  reconcileFailure(commandId, events, error) {
    assertNonEmptyString(commandId, "commandId");

    const record = this.#commands.get(commandId);

    if (!record || record.status !== COMMAND_STATUSES.FAILED) {
      throw new Error(`Command ${commandId} is not failed`);
    }

    if (!error || typeof error !== "object" || Array.isArray(error)) {
      throw new TypeError("error must be an object");
    }

    const eventRange = buildEventRange(events, commandId);
    const clonedError = clone(error);

    record.aggregateId = eventRange.aggregateId;
    record.eventRange = eventRange;
    record.error = clonedError;

    return clone(record);
  }

  #requireProcessing(commandId) {
    assertNonEmptyString(commandId, "commandId");

    const record = this.#commands.get(commandId);

    if (!record) {
      throw new Error(`Command ${commandId} is not reserved`);
    }

    if (record.status !== COMMAND_STATUSES.PROCESSING) {
      throw new Error(`Command ${commandId} is not processing`);
    }

    return record;
  }
}

module.exports = {
  COMMAND_STATUSES,
  InMemoryCommandStore,
};
