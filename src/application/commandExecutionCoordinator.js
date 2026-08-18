const { isDeepStrictEqual } = require("node:util");
const { randomUUID } = require("node:crypto");

const {
  COMMAND_STATUSES,
  assertCommandStoreAdapter,
  assertEventStoreAdapter,
} = require("./storeContracts");
const {
  DIAGNOSTIC_STATUSES,
  DIAGNOSTIC_TYPES,
  createDiagnosticEmitter,
} = require("./diagnostics");
const {
  createIdempotencyConflictError,
  createCommandInProgressError,
  createPartiallyCommittedCommandError,
  createCommandHistoryInconsistentError,
  createInterruptedCommandError,
  createCommandStatePersistenceError,
  createAppendCommitUnknownError,
  createCommandReconciliationError,
  createFencingTokenStaleError,
  createFencingContextInvalidError,
  serializeCommandError,
  deserializeCommandError,
} = require("./errors");

function assertIdentifier(value, name) {
  if (
    !(
      (typeof value === "string" && value.trim().length > 0) ||
      (Number.isSafeInteger(value) && value > 0)
    )
  ) {
    throw new TypeError(
      `${name} must be a non-empty string or a positive safe integer`
    );
  }
}

function assertOptionalIdentifier(value, name) {
  if (value !== undefined) {
    assertIdentifier(value, name);
  }
}

function normalizeCommandOptions(options = {}) {
  if (!options || typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError("command options must be an object");
  }

  const { commandId, correlationId, causationId } = options;

  if (
    commandId !== undefined &&
    (typeof commandId !== "string" || commandId.trim().length === 0)
  ) {
    throw new TypeError("commandId must be a non-empty string");
  }

  assertOptionalIdentifier(correlationId, "correlationId");
  assertOptionalIdentifier(causationId, "causationId");

  return { commandId, correlationId, causationId };
}

const DETERMINISTIC_COMMAND_REJECTION_CODES = new Set([
  "AGGREGATE_NOT_FOUND",
  "COMPENSATION_REQUIRED",
]);

const RECONCILABLE_UNKNOWN_COMMAND_CODES = new Set([
  "COMMAND_RECONCILIATION_FAILED",
  "EVENT_APPEND_COMMIT_UNKNOWN",
]);

class CommandExecutionCoordinator {
  #eventStore;

  #commandStore;

  #operationIdGenerator;

  #workerId;

  #leaseTtlMs;

  #clock;

  #now;

  #emitDiagnostic;

  constructor({
    eventStore,
    commandStore,
    operationIdGenerator,
    workerId = `worker-${process.pid}-${randomUUID().slice(0, 8)}`,
    leaseTtlMs = 5000,
    clock = () => new Date().toISOString(),
    now = null,
    diagnosticReporter,
    emitDiagnostic,
  }) {
    if (typeof operationIdGenerator !== "function") {
      throw new TypeError("operationIdGenerator must be a function");
    }

    this.#eventStore = assertEventStoreAdapter(eventStore);
    this.#commandStore = assertCommandStoreAdapter(commandStore);
    this.#operationIdGenerator = operationIdGenerator;
    this.#workerId = String(workerId);
    this.#leaseTtlMs = Number.isSafeInteger(leaseTtlMs) && leaseTtlMs > 0 ? leaseTtlMs : 30000;
    this.#clock = typeof clock === "function" ? clock : () => new Date().toISOString();
    this.#now = typeof now === "function" ? now : () => Date.now();
    this.#emitDiagnostic =
      emitDiagnostic ?? createDiagnosticEmitter(diagnosticReporter);
  }

  #getNow() {
    return this.#now();
  }

  execute(commandType, payload, options, executeCommand) {
    const normalizedOptions = normalizeCommandOptions(options);
    const { commandId } = normalizedOptions;
    const nowMs = this.#getNow();
    const workerId = this.#workerId;
    const leaseTtlMs = this.#leaseTtlMs;

    let currentFencingToken = 1;

    if (commandId) {
      const reservation = this.#commandStore.reserve({
        commandId,
        commandType,
        payload,
        workerId,
        leaseTtlMs,
        now: nowMs,
      });

      if (reservation.conflict) {
        throw createIdempotencyConflictError(commandId);
      }

      if (!reservation.created) {
        return this.#resolveExistingCommand(
          reservation.record,
          commandType,
          payload,
          normalizedOptions,
          executeCommand
        );
      }

      currentFencingToken = reservation.record.leaseToken || 1;
      this.#reconcileNewCommandReservation(commandId, currentFencingToken);
    }

    const eventCommandId = commandId ?? this.#operationIdGenerator();
    assertIdentifier(eventCommandId, "generated command ID");

    const commandContext = {
      commandId,
      eventCommandId,
      correlationId: normalizedOptions.correlationId ?? eventCommandId,
      initialCausationId: normalizedOptions.causationId ?? eventCommandId,
      lastEventId: null,
      committedEvents: [],
      workerId,
      fencingToken: currentFencingToken,
    };

    try {
      const result = executeCommand(commandContext);

      if (commandId) {
        this.#commandStore.complete(commandId, result, { fencingToken: currentFencingToken });
      }

      return result;
    } catch (caughtError) {
      if (caughtError?.code === "OPTIMISTIC_CONCURRENCY_CONFLICT") {
        this.#emitDiagnostic({
          type: DIAGNOSTIC_TYPES.CONCURRENCY_CONFLICT,
          status: DIAGNOSTIC_STATUSES.CONFLICT_DETECTED,
          commandId,
          aggregateId: caughtError.aggregateId,
          expectedVersion: caughtError.expectedVersion,
          actualVersion: caughtError.actualVersion,
        });
      } else if (caughtError?.code === "FENCING_TOKEN_STALE") {
        this.#emitDiagnostic({
          type: DIAGNOSTIC_TYPES.FENCING_REJECTION,
          status: DIAGNOSTIC_STATUSES.FENCING_TOKEN_STALE,
          commandId,
          providedToken: caughtError.providedToken,
          currentToken: caughtError.currentToken,
          workerId: this.#workerId,
        });
      } else if (caughtError?.code === "COMMAND_LEASE_EXPIRED") {
        this.#emitDiagnostic({
          type: DIAGNOSTIC_TYPES.COMMAND_LEASE,
          status: DIAGNOSTIC_STATUSES.LEASE_EXPIRED,
          commandId,
          fencingToken: caughtError.fencingToken,
          workerId: this.#workerId,
        });
      }

      return this.#handleExecutionError(caughtError, commandContext);
    }
  }

  commitEvent(event, commandContext, expectedVersion) {
    // Only pass fencing token when command has a reserved row (keyed commands)
    const appendFencingToken = commandContext.commandId ? commandContext.fencingToken : undefined;

    if (commandContext.commandId) {
      this.#commandStore.renewLease({
        commandId: commandContext.commandId,
        workerId: commandContext.workerId,
        fencingToken: commandContext.fencingToken,
        leaseTtlMs: this.#leaseTtlMs,
        now: this.#getNow(),
      });
    }

    const storedEvent = this.#appendEvent(event, expectedVersion, appendFencingToken);

    commandContext.lastEventId = storedEvent.eventId;
    commandContext.committedEvents.push(storedEvent);

    if (commandContext.commandId) {
      this.#commandStore.recordEvent(commandContext.commandId, storedEvent, {
        fencingToken: commandContext.fencingToken,
      });
    }

    return storedEvent;
  }

  #handleExecutionError(caughtError, commandContext) {
    const { commandId, committedEvents, fencingToken } = commandContext;

    if (!commandId) {
      if (committedEvents.length > 0) {
        let committedError = caughtError;

        if (caughtError.eventCommitted === true) {
          committedError.retrySafe = false;
          committedError.retryAction ??= "MANUAL_RESOLUTION_REQUIRED";
          committedError.aggregateId ??= committedEvents[0].aggregateId;
          committedError.eventIds ??= committedEvents.map(
            (event) => event.eventId
          );
        } else {
          committedError = createPartiallyCommittedCommandError(
            undefined,
            committedEvents,
            caughtError
          );
        }

        throw committedError;
      }

      if (caughtError.eventCommitted === true) {
        caughtError.retrySafe = false;
        caughtError.retryAction ??= "MANUAL_RESOLUTION_REQUIRED";
        throw caughtError;
      }

      if (caughtError.code === "EVENT_APPEND_COMMIT_UNKNOWN") {
        caughtError.retryAction = "MANUAL_RESOLUTION_REQUIRED";
      } else {
        caughtError.eventCommitted = false;
        caughtError.retrySafe = true;
      }

      throw caughtError;
    }

    if (committedEvents.length === 0) {
      caughtError.commandId = commandId;

      if (caughtError.eventCommitted === true) {
        caughtError.retrySafe = false;
        caughtError.retryAction ??= "MANUAL_RESOLUTION_REQUIRED";
      } else if (caughtError.code === "EVENT_APPEND_COMMIT_UNKNOWN") {
        caughtError.eventCommitted = null;
        caughtError.retrySafe = false;
        caughtError.retryAction = "RECONCILE_SAME_KEY";
        this.#persistFailedCommand(commandId, caughtError, [], { fencingToken });
      } else if (caughtError.code === "FENCING_TOKEN_STALE" || caughtError.code === "COMMAND_LEASE_EXPIRED" || caughtError.code === "FENCING_TOKEN_REQUIRED" || caughtError.code === "FENCING_CONTEXT_INVALID") {
        // Stale or expired worker must not release the command reservation
        throw caughtError;
      } else if (DETERMINISTIC_COMMAND_REJECTION_CODES.has(caughtError.code)) {
        caughtError.eventCommitted = false;
        caughtError.retrySafe = false;
        caughtError.retryAction = "REPLAY_SAME_KEY";
        this.#persistFailedCommand(commandId, caughtError, [], { fencingToken });
      } else {
        caughtError.eventCommitted = false;
        caughtError.retrySafe = true;
        caughtError.retryAction = "RETRY_SAME_KEY";

        try {
          this.#commandStore.release(commandId, { fencingToken });
        } catch (releaseError) {
          // The reservation could not be given up because this execution no
          // longer owns it. That is a technical consequence, not the reason the
          // command failed, so the original cause stays the primary answer and
          // the lease loss is attached underneath it.
          caughtError.cause ??= releaseError;
        }
      }

      throw caughtError;
    }

    let committedError = caughtError;

    if (caughtError.eventCommitted === true) {
      committedError.commandId = commandId;
      committedError.retrySafe = false;
      committedError.retryAction ??= "MANUAL_RESOLUTION_REQUIRED";
      committedError.aggregateId ??= committedEvents[0].aggregateId;
      committedError.eventIds ??= committedEvents.map((event) => event.eventId);
    } else {
      committedError = createPartiallyCommittedCommandError(
        commandId,
        committedEvents,
        caughtError
      );
    }

    this.#persistFailedCommand(commandId, committedError, committedEvents, { fencingToken });

    throw committedError;
  }

  #reconcileNewCommandReservation(commandId, fencingToken) {
    let events;

    try {
      events = this.#getEventsForCommand(commandId);
    } catch (reconciliationError) {
      this.#persistFailureWithoutEventReconciliation(
        commandId,
        reconciliationError,
        [],
        { fencingToken }
      );
      throw reconciliationError;
    }

    if (events.length === 0) {
      return;
    }

    if (!this.#commandEventsFormContiguousRange(commandId, events)) {
      const inconsistentError = createCommandHistoryInconsistentError(
        commandId,
        events
      );
      this.#persistFailureWithoutEventReconciliation(
        commandId,
        inconsistentError,
        events,
        { fencingToken }
      );
      throw inconsistentError;
    }

    const interruptedError = createInterruptedCommandError(commandId, events);
    this.#persistFailedCommand(commandId, interruptedError, events, {
      fencingToken,
    });
    throw interruptedError;
  }

  #resolveExistingCommand(
    record,
    commandType,
    payload,
    normalizedOptions,
    executeCommand
  ) {
    const events = this.#getEventsForCommand(record.commandId);

    if (record.status === COMMAND_STATUSES.COMPLETED) {
      const validEventlessCompensation =
        commandType === "COMPENSATE_CHECKOUT" &&
        record.eventRange === null &&
        events.length === 0 &&
        Array.isArray(record.result?.events) &&
        record.result.events.length === 0;

      if (
        !validEventlessCompensation &&
        !this.#eventRangeMatches(record.commandId, record.eventRange, events)
      ) {
        throw createCommandHistoryInconsistentError(record.commandId, events);
      }

      this.#emitDiagnostic({
        type: DIAGNOSTIC_TYPES.IDEMPOTENCY_DEDUPLICATED,
        status: DIAGNOSTIC_STATUSES.DEDUPLICATED,
        commandId: record.commandId,
        commandType,
      });

      return record.result;
    }

    if (record.status === COMMAND_STATUSES.PROCESSING) {
      if (events.length === 0) {
        if (record.eventRange) {
          throw createCommandHistoryInconsistentError(record.commandId, events);
        }

        const nowMs = this.#getNow();
        const expiresAt =
          record.leaseExpiresAt !== null && record.leaseExpiresAt !== undefined
            ? Number(record.leaseExpiresAt)
            : null;

        // If lease is still valid, return command in progress
        if (expiresAt !== null && expiresAt > nowMs) {
          throw createCommandInProgressError(record);
        }

        // If lease is expired and 0 events exist: safe takeover allowed!
        if (expiresAt !== null && expiresAt <= nowMs) {
          const takeover = this.#commandStore.takeOverExpired({
            commandId: record.commandId,
            workerId: this.#workerId,
            leaseTtlMs: this.#leaseTtlMs,
            now: nowMs,
            expectedToken: record.leaseToken,
          });

          if (takeover.success) {
            const newToken = takeover.record.leaseToken;
            this.#emitDiagnostic({
              type: DIAGNOSTIC_TYPES.COMMAND_LEASE,
              status: DIAGNOSTIC_STATUSES.LEASE_TAKEN_OVER,
              commandId: record.commandId,
              workerId: this.#workerId,
              fencingToken: newToken,
              previousToken: record.leaseToken,
            });

            const eventCommandId = record.commandId;
            const commandContext = {
              commandId: record.commandId,
              eventCommandId,
              correlationId: normalizedOptions.correlationId ?? eventCommandId,
              initialCausationId: normalizedOptions.causationId ?? eventCommandId,
              lastEventId: null,
              committedEvents: [],
              workerId: this.#workerId,
              fencingToken: newToken,
            };

            try {
              const result = executeCommand(commandContext);
              this.#commandStore.complete(record.commandId, result, { fencingToken: newToken });
              return result;
            } catch (caughtError) {
              return this.#handleExecutionError(caughtError, commandContext);
            }
          }

          // If takeover failed due to race, re-evaluate existing command
          return this.#resolveExistingCommand(
            this.#commandStore.get(record.commandId),
            commandType,
            payload,
            normalizedOptions,
            executeCommand
          );
        }

        throw createCommandInProgressError(record);
      }

      const nowMs = this.#getNow();
      const expiresAt =
        record.leaseExpiresAt !== null && record.leaseExpiresAt !== undefined
          ? Number(record.leaseExpiresAt)
          : null;

      // If events.length >= 1: partial-commit reconciliation. NO TAKEOVER!
      // But only abort if the owner's lease has actually expired!
      if (expiresAt !== null && expiresAt > nowMs) {
        throw createCommandInProgressError(record);
      }

      if (!this.#commandEventsFormContiguousRange(record.commandId, events)) {
        const inconsistentError = createCommandHistoryInconsistentError(
          record.commandId,
          events
        );
        this.#persistFailureWithoutEventReconciliation(
          record.commandId,
          inconsistentError,
          events,
          { fencingToken: record.leaseToken }
        );
        throw inconsistentError;
      }

      // Third-party authority revocation. Our expiry observation above is only
      // a reason to *try*: the store re-validates status, generation, expiry and
      // the authoritative event history inside one transaction and is the sole
      // decider. If the owner renewed in the meantime, it survives (LA-13).
      const interruptedError = createInterruptedCommandError(
        record.commandId,
        events
      );
      const revocation = this.#commandStore.revokeExpired({
        commandId: record.commandId,
        expectedToken: record.leaseToken,
        now: nowMs,
        error: serializeCommandError(interruptedError),
      });

      if (!revocation.success) {
        if (revocation.reason === "NOT_EXPIRED") {
          throw createCommandInProgressError(record);
        }

        // Status or generation moved underneath us; re-read and act on the
        // state that actually exists now.
        return this.#resolveExistingCommand(
          this.#commandStore.get(record.commandId),
          commandType,
          payload,
          normalizedOptions,
          executeCommand
        );
      }

      this.#emitDiagnostic({
        type: DIAGNOSTIC_TYPES.COMMAND_LEASE,
        status: DIAGNOSTIC_STATUSES.LEASE_REVOKED,
        commandId: record.commandId,
        fencingToken: record.leaseToken,
        workerId: this.#workerId,
      });

      throw deserializeCommandError(revocation.record.error);
    }

    if (record.status === COMMAND_STATUSES.FAILED) {
      if (RECONCILABLE_UNKNOWN_COMMAND_CODES.has(record.error?.code)) {
        if (events.length === 0) {
          // The recovering worker never held this lease, so it presents the
          // generation it observed. If another worker resolved the row first,
          // the generation moved and this release is rejected (G2).
          this.#commandStore.releaseFailed(record.commandId, record.error.code, {
            fencingToken: record.leaseToken,
          });
          return this.execute(
            commandType,
            payload,
            normalizedOptions,
            executeCommand
          );
        }

        if (!this.#commandEventsFormContiguousRange(record.commandId, events)) {
          throw createCommandHistoryInconsistentError(record.commandId, events);
        }

        const interruptedError = createInterruptedCommandError(
          record.commandId,
          events
        );

        try {
          this.#commandStore.reconcileFailure(
            record.commandId,
            events,
            serializeCommandError(interruptedError),
            { fencingToken: record.leaseToken }
          );
        } catch (persistenceError) {
          throw createCommandStatePersistenceError(
            record.commandId,
            events,
            persistenceError,
            true
          );
        }

        throw interruptedError;
      }

      const validEventlessFailure = record.eventRange === null && events.length === 0;

      if (
        !validEventlessFailure &&
        !this.#eventRangeMatches(record.commandId, record.eventRange, events)
      ) {
        throw createCommandHistoryInconsistentError(record.commandId, events);
      }

      throw deserializeCommandError(record.error);
    }

    throw createCommandHistoryInconsistentError(record.commandId, events);
  }

  #getEventsForCommand(commandId) {
    try {
      return this.#eventStore.getByCommandId(commandId);
    } catch (error) {
      this.#emitDiagnostic({
        type: DIAGNOSTIC_TYPES.COMMAND_RECONCILIATION,
        status: DIAGNOSTIC_STATUSES.LOOKUP_FAILED,
        commandId,
      });
      throw createCommandReconciliationError(commandId, error);
    }
  }

  #commandEventsFormContiguousRange(commandId, events) {
    if (events.length === 0) {
      return false;
    }

    const [firstEvent] = events;

    return events.every(
      (event, index) =>
        event.metadata?.commandId === commandId &&
        event.aggregateId === firstEvent.aggregateId &&
        event.sequence === firstEvent.sequence + index
    );
  }

  #eventRangeMatches(commandId, eventRange, events) {
    if (
      !eventRange ||
      !this.#commandEventsFormContiguousRange(commandId, events)
    ) {
      return false;
    }

    return (
      eventRange.aggregateId === events[0].aggregateId &&
      eventRange.firstSequence === events[0].sequence &&
      eventRange.lastSequence === events[events.length - 1].sequence &&
      isDeepStrictEqual(
        eventRange.eventIds,
        events.map((event) => event.eventId)
      )
    );
  }

  #persistFailedCommand(commandId, error, events, { fencingToken } = {}) {
    try {
      if (events.length > 0) {
        this.#commandStore.reconcileEvents(commandId, events, { fencingToken });
      }

      this.#commandStore.fail(commandId, serializeCommandError(error), { fencingToken });
    } catch (persistenceError) {
      throw this.#reconcilePersistenceFailure(
        commandId,
        events,
        error,
        persistenceError,
        fencingToken
      );
    }
  }

  /**
   * A refused error-persistence attempt is not automatically a persistence
   * problem. The write can equally have been refused because a third party
   * already established the terminal truth for this command, in which case
   * nothing failed at all. Only the persisted state can tell the two apart, so
   * this is a pure truth read: no mutation, no retry, no status change, no
   * generation change.
   *
   * The answer a caller receives must describe the state that is actually
   * persisted, not the mechanism by which the caller discovered its loss - a
   * losing owner and a retrying client that find the same state get the same
   * answer.
   */
  #reconcilePersistenceFailure(
    commandId,
    events,
    error,
    persistenceError,
    fencingToken
  ) {
    const persistenceFailure = () =>
      createCommandStatePersistenceError(
        commandId,
        events,
        persistenceError,
        error.eventCommitted
      );

    let record;

    try {
      record = this.#commandStore.get(commandId);
    } catch {
      // The truth read itself failed: it must not obscure the original cause.
      return persistenceFailure();
    }

    if (!record) {
      return persistenceFailure();
    }

    // A lost generation is the more precise diagnosis whatever the status is,
    // so it is checked first. Revocation leaves the generation untouched, so a
    // differing token always means a takeover replaced this execution.
    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      return createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        workerId: this.#workerId,
        leaseOwner: record.leaseOwner,
      });
    }

    // Someone else terminalised this command under our own generation, i.e. it
    // was revoked. Their record is the truth; hand it back unchanged.
    if (record.status !== COMMAND_STATUSES.PROCESSING && record.error) {
      return deserializeCommandError(record.error);
    }

    // The row is still ours and still processing: the write genuinely failed.
    return persistenceFailure();
  }

  #persistFailureWithoutEventReconciliation(
    commandId,
    error,
    events,
    { fencingToken } = {}
  ) {
    try {
      this.#commandStore.fail(commandId, serializeCommandError(error), {
        fencingToken,
      });
    } catch (persistenceError) {
      throw this.#reconcilePersistenceFailure(
        commandId,
        events,
        error,
        persistenceError,
        fencingToken
      );
    }
  }

  #appendEvent(event, expectedVersion, fencingToken) {
    try {
      return this.#eventStore.append(event, { expectedVersion, fencingToken });
    } catch (appendError) {
      // If append failed because fencing token was stale or lease expired, rethrow immediately
      if (appendError?.code === "FENCING_TOKEN_STALE" || appendError?.code === "COMMAND_LEASE_EXPIRED" || appendError?.code === "FENCING_TOKEN_REQUIRED" || appendError?.code === "FENCING_CONTEXT_INVALID") {
        throw appendError;
      }

      let commandEvents;

      try {
        commandEvents = this.#eventStore.getByCommandId(event.metadata.commandId);
      } catch (lookupError) {
        this.#emitDiagnostic({
          type: DIAGNOSTIC_TYPES.EVENT_APPEND,
          status: DIAGNOSTIC_STATUSES.COMMIT_UNKNOWN,
          commandId: event.metadata.commandId,
          aggregateId: event.aggregateId,
          eventId: event.eventId,
        });
        throw createAppendCommitUnknownError(event, appendError, lookupError);
      }

      const committedEvent = commandEvents.find(
        (candidate) => candidate.eventId === event.eventId
      );

      if (!committedEvent) {
        throw appendError;
      }

      if (!isDeepStrictEqual(committedEvent, event)) {
        throw createCommandHistoryInconsistentError(
          event.metadata.commandId,
          commandEvents
        );
      }

      this.#emitDiagnostic({
        type: DIAGNOSTIC_TYPES.EVENT_APPEND,
        status: DIAGNOSTIC_STATUSES.COMMIT_CONFIRMED_AFTER_ERROR,
        commandId: event.metadata.commandId,
        aggregateId: event.aggregateId,
        eventId: event.eventId,
      });

      return committedEvent;
    }
  }
}

module.exports = {
  CommandExecutionCoordinator,
};
