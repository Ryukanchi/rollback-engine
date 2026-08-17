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
  error.aggregateId = events[0]?.aggregateId;
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
  error.aggregateId = events[0]?.aggregateId;
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

function createFencingTokenStaleError({
  commandId,
  providedToken,
  currentToken,
  workerId,
  leaseOwner,
}) {
  const error = new Error(
    `Command ${commandId} fencing token ${providedToken} is stale (current token: ${currentToken}).`
  );
  error.code = "FENCING_TOKEN_STALE";
  error.commandId = commandId;
  error.providedToken = providedToken;
  error.currentToken = currentToken;
  if (workerId !== undefined) error.workerId = workerId;
  if (leaseOwner !== undefined) error.leaseOwner = leaseOwner;
  error.eventCommitted = false;
  error.retrySafe = false;
  error.retryAction = "ACQUIRE_NEW_LEASE";
  return error;
}

function createFencingTokenRequiredError({
  commandId,
  workerId,
  leaseOwner,
  message,
} = {}) {
  const error = new Error(
    message || `Command ${commandId} is leased and requires a fencing token to append events.`
  );
  error.code = "FENCING_TOKEN_REQUIRED";
  error.commandId = commandId;
  if (workerId !== undefined) error.workerId = workerId;
  if (leaseOwner !== undefined) error.leaseOwner = leaseOwner;
  error.eventCommitted = false;
  error.retrySafe = false;
  error.retryAction = "ACQUIRE_NEW_LEASE";
  return error;
}

function createFencingContextInvalidError({
  commandId,
  fencingToken,
  message,
} = {}) {
  const error = new Error(
    message || `Command ${commandId} fencing context is invalid: command record not found but fencing token was provided.`
  );
  error.code = "FENCING_CONTEXT_INVALID";
  error.commandId = commandId;
  error.fencingToken = fencingToken;
  error.eventCommitted = false;
  error.retrySafe = false;
  error.retryAction = "MANUAL_RESOLUTION_REQUIRED";
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
    "providedToken",
    "currentToken",
    "workerId",
    "leaseOwner",
    "fencingToken",
    "leaseExpiresAt",
    "now",
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

module.exports = {
  createIdempotencyConflictError,
  createCommandInProgressError,
  createPartiallyCommittedCommandError,
  createCommandHistoryInconsistentError,
  createInterruptedCommandError,
  createCommandStatePersistenceError,
  createAppendCommitUnknownError,
  createCommandReconciliationError,
  createFencingTokenStaleError,
  createFencingTokenRequiredError,
  createFencingContextInvalidError,
  serializeCommandError,
  deserializeCommandError,
};
