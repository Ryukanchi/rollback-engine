const CLASSIFICATIONS = {
  SUCCESS: "SUCCESS",
  EXPECTED_DOMAIN_REJECTION: "EXPECTED_DOMAIN_REJECTION",
  EXPECTED_IDEMPOTENCY_CONFLICT: "EXPECTED_IDEMPOTENCY_CONFLICT",
  EXPECTED_CONCURRENCY_CONFLICT: "EXPECTED_CONCURRENCY_CONFLICT",
  EXPECTED_KNOWN_BOUNDARY: "EXPECTED_KNOWN_BOUNDARY",
  EXPECTED_FAULT_INJECTION: "EXPECTED_FAULT_INJECTION",
  EXPECTED_INTERRUPTED_COMMIT: "EXPECTED_INTERRUPTED_COMMIT",
  UNEXPECTED_ENGINE_ERROR: "UNEXPECTED_ENGINE_ERROR",
  INVARIANT_VIOLATION: "INVARIANT_VIOLATION",
};

const EXPECTED_DOMAIN_ERROR_CODES = new Set([
  "AGGREGATE_NOT_FOUND",
  "INVALID_ORDER_COMMAND",
  "INVALID_CHECKOUT_COMMAND",
  "COMPENSATION_REQUIRED",
  "INVALID_LIFECYCLE_TRANSITION",
  "INVALID_EVENT_SEQUENCE",
  "INVALID_EVENT_SCHEMA",
]);

const EXPECTED_IDEMPOTENCY_ERROR_CODES = new Set([
  "COMMAND_PAYLOAD_MISMATCH",
  "IDEMPOTENCY_KEY_CONFLICT",
  "COMMAND_ALREADY_EXISTS",
]);

const EXPECTED_CONCURRENCY_ERROR_CODES = new Set([
  "OPTIMISTIC_LOCK_CONFLICT",
  "STALE_EXPECTED_VERSION",
  "VERSION_MISMATCH",
]);

function classifyOutcome({ success, error, context = {} }) {
  if (success) {
    return CLASSIFICATIONS.SUCCESS;
  }

  if (!error) {
    return CLASSIFICATIONS.SUCCESS;
  }

  const code = error.code || error.name || "";
  const msg = error.message || "";

  // 1. Known processing + 0 boundary
  if (code === "COMMAND_IN_PROGRESS" || msg.includes("already being processed")) {
    return CLASSIFICATIONS.EXPECTED_KNOWN_BOUNDARY;
  }

  // 2. Interrupted commit / lost ACK reconciliation
  if (code === "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" || msg.includes("interrupted after commit")) {
    return CLASSIFICATIONS.EXPECTED_INTERRUPTED_COMMIT;
  }

  // 3. Idempotency Payload mismatch
  if (EXPECTED_IDEMPOTENCY_ERROR_CODES.has(code) || msg.includes("different command payload")) {
    return CLASSIFICATIONS.EXPECTED_IDEMPOTENCY_CONFLICT;
  }

  // 4. Optimistic concurrency conflicts
  if (EXPECTED_CONCURRENCY_ERROR_CODES.has(code) || msg.includes("expected version")) {
    return CLASSIFICATIONS.EXPECTED_CONCURRENCY_CONFLICT;
  }

  // 5. Injected saga fault
  if (code.startsWith("SIMULATED_") || msg.includes("Simulated failure") || context.isFaultInjected) {
    return CLASSIFICATIONS.EXPECTED_FAULT_INJECTION;
  }

  // 6. Domain validations and invalid transitions
  if (EXPECTED_DOMAIN_ERROR_CODES.has(code) || error instanceof TypeError || error instanceof RangeError) {
    return CLASSIFICATIONS.EXPECTED_DOMAIN_REJECTION;
  }

  return CLASSIFICATIONS.UNEXPECTED_ENGINE_ERROR;
}

module.exports = {
  CLASSIFICATIONS,
  classifyOutcome,
};
