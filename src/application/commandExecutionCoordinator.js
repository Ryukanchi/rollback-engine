const { isDeepStrictEqual } = require("node:util");

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

function createIdempotencyConflictError(commandId) {
  const error = new Error(
    "The idempotency key was already used for a different command."
  );
  error.code = "IDEMPOTENCY_KEY_CONFLICT";
  error.commandId = commandId;
  error.eventCommitted = false;
  error.retrySafe = false;
  error.retryAction = "USE_NEW_KEY";
  return error;
}

function createCommandInProgressError(record) {
  const error = new Error("The command is already being processed.");
  error.code = "COMMAND_IN_PROGRESS";
  error.commandId = record.commandId;
  error.eventCommitted = false;
  error.retrySafe = false;
  error.retryAction = "WAIT_AND_RETRY_SAME_KEY";

  return error;
}

function createPartiallyCommittedCommandError(commandId, events, cause) {
  const error = new Error(
    "The command committed one or more events but did not complete.",
    { cause }
  );
  error.code = "COMMAND_EXECUTION_PARTIALLY_COMMITTED";
  error.commandId = commandId;
  error.aggregateId = events[0].aggregateId;
  error.eventIds = events.map((event) => event.eventId);
  error.eventCommitted = true;
  error.retrySafe = false;
  error.retryAction = "MANUAL_RESOLUTION_REQUIRED";
  return error;
}

function createCommandHistoryInconsistentError(commandId, events = []) {
  const error = new Error(
    "The command record does not match the authoritative event history."
  );
  error.code = "COMMAND_EVENT_HISTORY_INCONSISTENT";
  error.commandId = commandId;
  error.eventCommitted = events.length > 0;
  error.retrySafe = false;
  error.retryAction = "MANUAL_RESOLUTION_REQUIRED";

  if (events.length > 0) {
    error.eventIds = events.map((event) => event.eventId);

    if (events.every((event) => event.aggregateId === events[0].aggregateId)) {
      error.aggregateId = events[0].aggregateId;
    }
  }

  return error;
}

function createInterruptedCommandError(commandId, events) {
  const error = new Error(
    "Committed command events were found without a completed command result."
  );
  error.code = "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT";
  error.commandId = commandId;
  error.aggregateId = events[0].aggregateId;
  error.eventIds = events.map((event) => event.eventId);
  error.eventCommitted = true;
  error.retrySafe = false;
  error.retryAction = "MANUAL_RESOLUTION_REQUIRED";
  return error;
}

function createCommandStatePersistenceError(
  commandId,
  events,
  cause,
  eventCommitted
) {
  const error = new Error(
    "The command state could not be persisted after command execution stopped.",
    { cause }
  );
  error.code = "COMMAND_STATE_PERSISTENCE_FAILED";
  error.commandId = commandId;
  error.eventCommitted =
    typeof eventCommitted === "boolean" ? eventCommitted : null;
  error.retrySafe = false;
  error.retryAction = "RECONCILE_SAME_KEY";

  if (events.length > 0) {
    error.aggregateId = events[0].aggregateId;
    error.eventIds = events.map((event) => event.eventId);
  }

  return error;
}

function createAppendCommitUnknownError(event, appendError, lookupError) {
  const error = new Error(
    "The event store did not confirm whether the event was committed.",
    { cause: appendError }
  );
  error.code = "EVENT_APPEND_COMMIT_UNKNOWN";
  error.commandId = event.metadata.commandId;
  error.aggregateId = event.aggregateId;
  error.eventId = event.eventId;
  error.eventCommitted = null;
  error.retrySafe = false;
  error.retryAction = "RECONCILE_SAME_KEY";
  error.reconciliationError = lookupError;
  return error;
}

function createCommandReconciliationError(commandId, cause) {
  const error = new Error(
    "The command could not be reconciled with the authoritative event history.",
    { cause }
  );
  error.code = "COMMAND_RECONCILIATION_FAILED";
  error.commandId = commandId;
  error.eventCommitted = null;
  error.retrySafe = false;
  error.retryAction = "RECONCILE_SAME_KEY";
  return error;
}

function serializeCommandError(error) {
  const serialized = {
    code: error.code,
    message: error.message,
    eventCommitted: error.eventCommitted,
    retrySafe: error.retrySafe,
    retryAction: error.retryAction,
  };

  for (const fieldName of [
    "commandId",
    "aggregateId",
    "eventId",
    "eventIds",
  ]) {
    if (error[fieldName] !== undefined) {
      serialized[fieldName] = error[fieldName];
    }
  }

  return serialized;
}

function deserializeCommandError(serialized) {
  const error = new Error(serialized.message);
  Object.assign(error, serialized);
  return error;
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

  #emitDiagnostic;

  constructor({
    eventStore,
    commandStore,
    operationIdGenerator,
    diagnosticReporter,
  }) {
    if (typeof operationIdGenerator !== "function") {
      throw new TypeError("operationIdGenerator must be a function");
    }

    this.#eventStore = assertEventStoreAdapter(eventStore);
    this.#commandStore = assertCommandStoreAdapter(commandStore);
    this.#operationIdGenerator = operationIdGenerator;
    this.#emitDiagnostic = createDiagnosticEmitter(diagnosticReporter);
  }

  execute(commandType, payload, options, executeCommand) {
    const normalizedOptions = normalizeCommandOptions(options);
    const { commandId } = normalizedOptions;

    if (commandId) {
      const reservation = this.#commandStore.reserve({
        commandId,
        commandType,
        payload,
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

      this.#reconcileNewCommandReservation(commandId);
    }

    const eventCommandId = commandId ?? this.#operationIdGenerator();
    assertIdentifier(eventCommandId, "generated command ID");

    const commandContext = {
      commandId,
      eventCommandId,
      correlationId:
        normalizedOptions.correlationId ?? eventCommandId,
      initialCausationId:
        normalizedOptions.causationId ?? eventCommandId,
      lastEventId: null,
      committedEvents: [],
    };

    try {
      const result = executeCommand(commandContext);

      if (commandId) {
        this.#commandStore.complete(commandId, result);
      }

      return result;
    } catch (caughtError) {
      return this.#handleExecutionError(caughtError, commandContext);
    }
  }

  commitEvent(event, commandContext, expectedVersion) {
    const storedEvent = this.#appendEvent(event, expectedVersion);

    commandContext.lastEventId = storedEvent.eventId;
    commandContext.committedEvents.push(storedEvent);

    if (commandContext.commandId) {
      this.#commandStore.recordEvent(commandContext.commandId, storedEvent);
    }

    return storedEvent;
  }

  #handleExecutionError(caughtError, commandContext) {
    const { commandId, committedEvents } = commandContext;

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
        this.#persistFailedCommand(commandId, caughtError, []);
      } else if (DETERMINISTIC_COMMAND_REJECTION_CODES.has(caughtError.code)) {
        caughtError.eventCommitted = false;
        caughtError.retrySafe = false;
        caughtError.retryAction = "REPLAY_SAME_KEY";
        this.#persistFailedCommand(commandId, caughtError, []);
      } else {
        caughtError.eventCommitted = false;
        caughtError.retrySafe = true;
        caughtError.retryAction = "RETRY_SAME_KEY";
        this.#commandStore.release(commandId);
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

    this.#persistFailedCommand(commandId, committedError, committedEvents);

    throw committedError;
  }

  #reconcileNewCommandReservation(commandId) {
    let events;

    try {
      events = this.#getEventsForCommand(commandId);
    } catch (reconciliationError) {
      this.#persistFailureWithoutEventReconciliation(
        commandId,
        reconciliationError,
        []
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
        events
      );
      throw inconsistentError;
    }

    const interruptedError = createInterruptedCommandError(commandId, events);
    this.#persistFailedCommand(commandId, interruptedError, events);
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

      return record.result;
    }

    if (record.status === COMMAND_STATUSES.PROCESSING) {
      if (events.length === 0) {
        if (record.eventRange) {
          throw createCommandHistoryInconsistentError(record.commandId, events);
        }

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
          events
        );
        throw inconsistentError;
      }

      const interruptedError = createInterruptedCommandError(
        record.commandId,
        events
      );
      this.#persistFailedCommand(record.commandId, interruptedError, events);
      throw interruptedError;
    }

    if (record.status === COMMAND_STATUSES.FAILED) {
      if (RECONCILABLE_UNKNOWN_COMMAND_CODES.has(record.error?.code)) {
        if (events.length === 0) {
          this.#commandStore.releaseFailed(record.commandId, record.error.code);
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
            serializeCommandError(interruptedError)
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

  #persistFailedCommand(commandId, error, events) {
    try {
      if (events.length > 0) {
        this.#commandStore.reconcileEvents(commandId, events);
      }

      this.#commandStore.fail(commandId, serializeCommandError(error));
    } catch (persistenceError) {
      throw createCommandStatePersistenceError(
        commandId,
        events,
        persistenceError,
        error.eventCommitted
      );
    }
  }

  #persistFailureWithoutEventReconciliation(commandId, error, events) {
    try {
      this.#commandStore.fail(commandId, serializeCommandError(error));
    } catch (persistenceError) {
      throw createCommandStatePersistenceError(
        commandId,
        events,
        persistenceError,
        error.eventCommitted
      );
    }
  }

  #appendEvent(event, expectedVersion) {
    try {
      return this.#eventStore.append(event, { expectedVersion });
    } catch (appendError) {
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
