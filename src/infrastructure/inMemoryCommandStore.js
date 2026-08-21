const { isDeepStrictEqual } = require("node:util");
const {
  COMMAND_STATUSES,
  assertCommandReceiptMetadata,
  assertLeaseTtlMs,
  createLeaseDeadline,
} = require("../application/storeContracts");
const {
  createFencingTokenStaleError,
  createFencingTokenRequiredError,
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

  #now;

  /**
   * The lease clock belongs to the Store, not to the caller of a mutation.
   * Creating, extending, transferring and revoking a lease are all decided
   * against this clock, so a caller can choose *which* challenge to attempt but
   * never *when* it is allowed to succeed (TA-3).
   */
  constructor({ now = () => Date.now() } = {}) {
    this.#now = typeof now === "function" ? now : () => Date.now();
  }

  setEventStore(eventStore) {
    this.#eventStore = eventStore;
  }

  reserve({
    commandId,
    commandType,
    payload,
    workerId = null,
    leaseTtlMs = 5000,
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(commandType, "commandType");
    assertPayload(payload);
    assertLeaseTtlMs(leaseTtlMs);

    const now = this.#now();
    // Built before the row is touched: a policy this store cannot turn into a
    // deadline must not leave a half-reserved command behind.
    const leaseExpiresAt = workerId ? createLeaseDeadline(now, leaseTtlMs) : null;

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

        existing.commandType = commandType;
        existing.payload = clone(payload);
        existing.status = COMMAND_STATUSES.PROCESSING;
        existing.aggregateId = null;
        existing.eventRange = null;
        existing.result = null;
        existing.receiptMetadata = null;
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

    const record = {
      commandId,
      commandType,
      payload: clone(payload),
      status: COMMAND_STATUSES.PROCESSING,
      aggregateId: null,
      eventRange: null,
      result: null,
      receiptMetadata: null,
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
    expectedToken,
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(workerId, "workerId");
    assertLeaseTtlMs(leaseTtlMs);

    const now = this.#now();
    const leaseExpiresAt = createLeaseDeadline(now, leaseTtlMs);

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
    record.leaseExpiresAt = leaseExpiresAt;

    return {
      success: true,
      record: clone(record),
    };
  }

  /**
   * Third-party authority revocation for a partially committed command. Logical
   * mirror of takeOverExpired(); see the SQLite adapter for the transactional
   * reasoning. In-memory mutation is synchronous and single-threaded, so this
   * whole method is the atomic unit that BEGIN IMMEDIATE provides there.
   *
   * The authoritative event history is mandatory here: unlike takeOverExpired(),
   * which can still fall back to the recorded eventRange, the 0-vs->=1 decision
   * and the persisted range must both come from the Event Store (LA-14). An
   * unwired adapter therefore cannot answer this question and is rejected rather
   * than silently answered from lagging bookkeeping.
   */
  revokeExpired({ commandId, expectedToken, error } = {}) {
    assertNonEmptyString(commandId, "commandId");

    if (!Number.isSafeInteger(expectedToken) || expectedToken <= 0) {
      throw new TypeError("expectedToken must be a positive safe integer");
    }

    if (!error || typeof error !== "object" || Array.isArray(error)) {
      throw new TypeError("error must be an object");
    }

    const now = this.#now();

    const record = this.#commands.get(commandId);

    if (!record) {
      return { success: false, reason: "NOT_FOUND" };
    }

    if (record.status !== COMMAND_STATUSES.PROCESSING) {
      return { success: false, reason: "NOT_PROCESSING" };
    }

    if (record.leaseToken !== expectedToken) {
      return { success: false, reason: "TOKEN_MISMATCH" };
    }

    // LA-13/TA-2: the caller observed expiry earlier and against some other
    // clock; only this read, against the Store clock, decides.
    if (record.leaseExpiresAt !== null && record.leaseExpiresAt > now) {
      return { success: false, reason: "NOT_EXPIRED" };
    }

    // LA-14: authoritative event history, never the recorded eventRange. The
    // dependency is only required once the decision actually needs it, so the
    // cheap eligibility answers stay identical to the SQLite adapter.
    if (!this.#eventStore) {
      throw new TypeError(
        "revokeExpired requires a wired Event Store to read authoritative events"
      );
    }

    const events = this.#eventStore.getByCommandId(commandId) || [];

    if (events.length === 0) {
      return { success: false, reason: "NO_EVENTS" };
    }

    const eventRange = buildEventRange(events, commandId);

    record.status = COMMAND_STATUSES.FAILED;
    record.aggregateId = eventRange.aggregateId;
    record.eventRange = eventRange;
    record.error = {
      ...clone(error),
      aggregateId: eventRange.aggregateId,
      eventIds: [...eventRange.eventIds],
    };
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    return { success: true, record: clone(record) };
  }

  renewLease({
    commandId,
    workerId,
    fencingToken,
    leaseTtlMs = 5000,
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(workerId, "workerId");
    assertLeaseTtlMs(leaseTtlMs);

    const now = this.#now();
    const leaseExpiresAt = createLeaseDeadline(now, leaseTtlMs);

    const record = this.#commands.get(commandId);

    if (!record || record.status !== COMMAND_STATUSES.PROCESSING) {
      throw new Error(`Command ${commandId} is not processing`);
    }

    // Renewal acts on an existing generation, so the caller has to name it.
    // The owner answers "which worker", the generation answers "which
    // reservation". Neither substitutes for the other: a long-lived worker
    // keeps its identity across generations of the same command, so an owner
    // match alone cannot tell generation N from generation N+1.
    this.#assertGeneration(commandId, record, fencingToken, workerId);

    if (record.leaseOwner !== workerId) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        workerId,
        leaseOwner: record.leaseOwner,
      });
    }

    // Lease expiry is a promise to third parties, not a self-permission. A
    // worker that still holds the current generation may renew after the
    // nominal TTL; only a committed takeover or revocation removes authority.
    record.leaseExpiresAt = leaseExpiresAt;
    return { renewed: true, leaseExpiresAt: record.leaseExpiresAt };
  }

  recordEvent(commandId, event, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);
    this.#assertGeneration(commandId, record, fencingToken);

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

  complete(commandId, result, { fencingToken, receiptMetadata } = {}) {
    const record = this.#requireProcessing(commandId);
    this.#assertGeneration(commandId, record, fencingToken);

    const clonedResult = clone(result);
    const clonedReceiptMetadata = clone(receiptMetadata);
    assertCommandReceiptMetadata(clonedReceiptMetadata);

    record.status = COMMAND_STATUSES.COMPLETED;
    record.result = clonedResult;
    record.receiptMetadata = clonedReceiptMetadata;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    return clone(record);
  }

  fail(commandId, error, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);
    this.#assertGeneration(commandId, record, fencingToken);

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

    this.#assertGeneration(commandId, record, fencingToken);

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

    this.#assertGeneration(commandId, record, fencingToken);

    // G1: the row is retained in a non-active state so the generation survives.
    // Deleting it would let the next reserve() restart at token 1 and revive a
    // zombie of the failed generation (ABA).
    record.status = COMMAND_STATUSES.RELEASED;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    return true;
  }

  get(commandId) {
    assertNonEmptyString(commandId, "commandId");

    return clone(this.#commands.get(commandId) ?? null);
  }

  reconcileEvents(commandId, events, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    // G4: repair authority belongs to the generation the caller observed.
    this.#assertGeneration(commandId, record, fencingToken);

    const eventRange = buildEventRange(events, commandId);

    record.aggregateId = eventRange.aggregateId;
    record.eventRange = eventRange;

    return clone(record);
  }

  reconcileFailure(commandId, events, error, { fencingToken } = {}) {
    assertNonEmptyString(commandId, "commandId");

    const record = this.#commands.get(commandId);

    if (!record || record.status !== COMMAND_STATUSES.FAILED) {
      throw new Error(`Command ${commandId} is not failed`);
    }

    // G4: a failed row may be repaired from the event history, but only by the
    // generation that observed it. Ownership is deliberately not required here,
    // because post-commit reconciliation is performed by a recovering worker.
    this.#assertGeneration(commandId, record, fencingToken);

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

  /**
   * G2: every mutation of an existing command row must name the generation it
   * believes it is acting on. An omitted token is rejected rather than treated
   * as "unfenced", because an unfenced write is indistinguishable from a stale
   * one once a takeover has happened.
   */
  #assertGeneration(commandId, record, fencingToken, workerId) {
    if (fencingToken === undefined || fencingToken === null) {
      throw createFencingTokenRequiredError({
        commandId,
        workerId,
        leaseOwner: record.leaseOwner,
        message: `Command ${commandId} requires a fencing token to be mutated.`,
      });
    }

    if (record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        workerId,
        leaseOwner: record.leaseOwner,
      });
    }
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
