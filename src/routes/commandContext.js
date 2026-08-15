const { createHttpError } = require("../middleware/errorHandler");

const MAX_IDENTIFIER_LENGTH = 200;

function readOptionalIdentifier(req, headerName) {
  const value = req.get(headerName);

  if (value === undefined) {
    return undefined;
  }

  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    value.length > MAX_IDENTIFIER_LENGTH
  ) {
    throw createHttpError(
      400,
      `${headerName} must be a non-empty string of at most ${MAX_IDENTIFIER_LENGTH} characters`
    );
  }

  return value;
}

function readCommandContext(req) {
  return {
    commandId: readOptionalIdentifier(req, "Idempotency-Key"),
    correlationId: readOptionalIdentifier(req, "X-Correlation-Id"),
    causationId: readOptionalIdentifier(req, "X-Causation-Id"),
  };
}

function setCommandResponseHeaders(res, commandContext) {
  if (commandContext.commandId) {
    res.set("Idempotency-Key", commandContext.commandId);
  }
}

module.exports = {
  readCommandContext,
  setCommandResponseHeaders,
};
