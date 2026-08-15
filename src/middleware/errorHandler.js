function createHttpError(statusCode, message, code) {
  const error = new Error(message);
  error.statusCode = statusCode;

  if (code) {
    error.code = code;
  }

  return error;
}

function notFoundHandler(req, res, next) {
  next(createHttpError(404, `Route ${req.method} ${req.originalUrl} not found`));
}

const PUBLIC_ERROR_DEFINITIONS = Object.freeze({
  AGGREGATE_NOT_FOUND: {
    statusCode: 404,
    category: "not_found",
    retryAction: "FIX_REQUEST",
  },
  COMPENSATION_REQUIRED: {
    statusCode: 409,
    category: "domain",
    retryAction: "COMPENSATE_THEN_RETRY",
  },
  EVENT_COMMITTED_VIEW_REPAIR_FAILED: {
    statusCode: 500,
    category: "technical",
    message:
      "The event was committed, but the materialized view could not be repaired.",
    retryAction: "MANUAL_RESOLUTION_REQUIRED",
  },
  IDEMPOTENCY_KEY_CONFLICT: {
    statusCode: 409,
    category: "conflict",
    message: "The idempotency key was already used for a different command.",
    retryAction: "USE_NEW_KEY",
  },
  COMMAND_IN_PROGRESS: {
    statusCode: 409,
    category: "conflict",
    message: "The command is already being processed.",
    retryAction: "WAIT_AND_RETRY_SAME_KEY",
  },
  COMMAND_EXECUTION_PARTIALLY_COMMITTED: {
    statusCode: 500,
    category: "technical",
    message: "The command committed one or more events but did not complete.",
    retryAction: "MANUAL_RESOLUTION_REQUIRED",
  },
  COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT: {
    statusCode: 500,
    category: "technical",
    message:
      "Committed command events were found without a completed command result.",
    retryAction: "MANUAL_RESOLUTION_REQUIRED",
  },
  COMMAND_EVENT_HISTORY_INCONSISTENT: {
    statusCode: 500,
    category: "technical",
    message:
      "The command record does not match the authoritative event history.",
    retryAction: "MANUAL_RESOLUTION_REQUIRED",
  },
  COMMAND_STATE_PERSISTENCE_FAILED: {
    statusCode: 500,
    category: "technical",
    message: "The command state could not be persisted.",
    retryAction: "RECONCILE_SAME_KEY",
  },
  EVENT_APPEND_COMMIT_UNKNOWN: {
    statusCode: 500,
    category: "technical",
    message: "The event store did not confirm whether the event was committed.",
    retryAction: "RECONCILE_SAME_KEY",
  },
  COMMAND_RECONCILIATION_FAILED: {
    statusCode: 500,
    category: "technical",
    message:
      "The command could not be reconciled with the authoritative event history.",
    retryAction: "RECONCILE_SAME_KEY",
  },
  OPTIMISTIC_CONCURRENCY_CONFLICT: {
    statusCode: 409,
    category: "conflict",
    message: "The aggregate changed before the event could be committed.",
    retryAction: "RETRY_COMMAND",
  },
});

function definitionForStatus(statusCode) {
  if (statusCode === 400) {
    return {
      code: "VALIDATION_ERROR",
      statusCode,
      category: "validation",
      retryAction: "FIX_REQUEST",
    };
  }

  if (statusCode === 404) {
    return {
      code: "NOT_FOUND",
      statusCode,
      category: "not_found",
      retryAction: "FIX_REQUEST",
    };
  }

  if (statusCode >= 500) {
    return {
      code: "INTERNAL_ERROR",
      statusCode,
      category: "technical",
      retryAction: "MANUAL_RESOLUTION_REQUIRED",
    };
  }

  return {
    code: "REQUEST_REJECTED",
    statusCode,
    category: "request",
    retryAction: "FIX_REQUEST",
  };
}

function resolveErrorDefinition(error) {
  if (Object.hasOwn(PUBLIC_ERROR_DEFINITIONS, error.code)) {
    return {
      code: error.code,
      ...PUBLIC_ERROR_DEFINITIONS[error.code],
    };
  }

  if (Number.isInteger(error.statusCode)) {
    return definitionForStatus(error.statusCode);
  }

  if (Number.isInteger(error.status)) {
    return definitionForStatus(error.status);
  }

  if (error instanceof TypeError) {
    return {
      code: "VALIDATION_ERROR",
      statusCode: 400,
      category: "validation",
      retryAction: "FIX_REQUEST",
    };
  }

  return {
    code: "INTERNAL_ERROR",
    statusCode: 500,
    category: "technical",
    retryAction: "MANUAL_RESOLUTION_REQUIRED",
  };
}

function errorHandler(error, req, res, next) {
  if (res.headersSent) {
    return next(error);
  }

  const definition = resolveErrorDefinition(error);
  const eventCommitted =
    typeof error.eventCommitted === "boolean"
      ? error.eventCommitted
      : definition.statusCode < 500
        ? false
        : null;
  const responseError = {
    code: definition.code,
    category: definition.category,
    message:
      definition.message ??
      (definition.statusCode >= 500 ? "Internal server error" : error.message),
    eventCommitted,
    retrySafe:
      typeof error.retrySafe === "boolean"
        ? error.retrySafe
        : eventCommitted === null
          ? null
          : !eventCommitted,
    retryAction:
      error.retryAction ??
      definition.retryAction ??
      "MANUAL_RESOLUTION_REQUIRED",
  };

  for (const fieldName of [
    "commandId",
    "aggregateId",
    "eventId",
    "eventIds",
  ]) {
    if (error[fieldName] !== undefined) {
      responseError[fieldName] = error[fieldName];
    }
  }

  if (eventCommitted === true && !Array.isArray(responseError.eventIds)) {
    responseError.eventIds =
      error.eventId === undefined ? [] : [error.eventId];
  }

  return res.status(definition.statusCode).json({ error: responseError });
}

module.exports = {
  createHttpError,
  errorHandler,
  notFoundHandler,
};
