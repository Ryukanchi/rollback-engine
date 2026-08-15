const DIAGNOSTIC_TYPES = Object.freeze({
  COMMAND_RECONCILIATION: "COMMAND_RECONCILIATION",
  EVENT_APPEND: "EVENT_APPEND",
  MATERIALIZED_VIEW_REPAIR: "MATERIALIZED_VIEW_REPAIR",
  SNAPSHOT_SAVE: "SNAPSHOT_SAVE",
  IDEMPOTENCY_DEDUPLICATED: "IDEMPOTENCY_DEDUPLICATED",
  CONCURRENCY_CONFLICT: "CONCURRENCY_CONFLICT",
  SNAPSHOT_FALLBACK_REPLAY: "SNAPSHOT_FALLBACK_REPLAY",
  SUBSCRIPTION_ERROR: "SUBSCRIPTION_ERROR",
  COMMAND_LEASE: "COMMAND_LEASE",
  FENCING_REJECTION: "FENCING_REJECTION",
});

const DIAGNOSTIC_STATUSES = Object.freeze({
  COMMIT_CONFIRMED_AFTER_ERROR: "COMMIT_CONFIRMED_AFTER_ERROR",
  COMMIT_UNKNOWN: "COMMIT_UNKNOWN",
  LOOKUP_FAILED: "LOOKUP_FAILED",
  REPAIRED: "REPAIRED",
  REPAIR_FAILED: "REPAIR_FAILED",
  SAVE_FAILED: "SAVE_FAILED",
  DEDUPLICATED: "DEDUPLICATED",
  CONFLICT_DETECTED: "CONFLICT_DETECTED",
  FALLBACK_TO_FULL_REPLAY: "FALLBACK_TO_FULL_REPLAY",
  HANDLER_FAILED: "HANDLER_FAILED",
  LEASE_ACQUIRED: "LEASE_ACQUIRED",
  LEASE_RENEWED: "LEASE_RENEWED",
  LEASE_TAKEN_OVER: "LEASE_TAKEN_OVER",
  LEASE_EXPIRED: "LEASE_EXPIRED",
  FENCING_TOKEN_STALE: "FENCING_TOKEN_STALE",
});

const DEFAULT_BUFFER_SIZE = 500;

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

class DiagnosticBuffer {
  #buffer = [];

  #maxSize;

  constructor(maxSize = DEFAULT_BUFFER_SIZE) {
    this.#maxSize = maxSize;
  }

  push(diagnostic) {
    this.#buffer.push(diagnostic);

    if (this.#buffer.length > this.#maxSize) {
      this.#buffer.shift();
    }
  }

  query({
    type,
    status,
    aggregateId,
    commandId,
    limit,
  } = {}) {
    let results = [...this.#buffer];

    if (type !== undefined) {
      results = results.filter((d) => d.type === type);
    }

    if (status !== undefined) {
      results = results.filter((d) => d.status === status);
    }

    if (aggregateId !== undefined) {
      results = results.filter(
        (d) => String(d.aggregateId) === String(aggregateId)
      );
    }

    if (commandId !== undefined) {
      results = results.filter((d) => d.commandId === commandId);
    }

    if (Number.isSafeInteger(limit) && limit > 0) {
      results = results.slice(-limit);
    }

    return results;
  }

  clear() {
    this.#buffer = [];
  }
}

function createDiagnosticEmitter(reporter = () => {}, { bufferSize = DEFAULT_BUFFER_SIZE } = {}) {
  if (typeof reporter !== "function") {
    throw new TypeError("diagnosticReporter must be a function");
  }

  const buffer = new DiagnosticBuffer(bufferSize);

  const emitter = function emitDiagnostic({ type, status, ...context }) {
    if (typeof type !== "string" || type.length === 0) {
      throw new TypeError("diagnostic.type must be a non-empty string");
    }

    if (typeof status !== "string" || status.length === 0) {
      throw new TypeError("diagnostic.status must be a non-empty string");
    }

    const diagnostic = deepFreeze(
      structuredClone({
        type,
        status,
        occurredAt: new Date().toISOString(),
        ...context,
      })
    );

    buffer.push(diagnostic);

    try {
      const outcome = reporter(diagnostic);

      if (outcome && typeof outcome.catch === "function") {
        outcome.catch(() => {});
      }
    } catch {
      // Diagnostics are best-effort and never part of the command boundary.
    }

    return diagnostic;
  };

  emitter.query = (filter) => buffer.query(filter);
  emitter.clear = () => buffer.clear();

  return emitter;
}

module.exports = {
  DIAGNOSTIC_STATUSES,
  DIAGNOSTIC_TYPES,
  DiagnosticBuffer,
  createDiagnosticEmitter,
};
