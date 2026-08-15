const DIAGNOSTIC_TYPES = Object.freeze({
  COMMAND_RECONCILIATION: "COMMAND_RECONCILIATION",
  EVENT_APPEND: "EVENT_APPEND",
  MATERIALIZED_VIEW_REPAIR: "MATERIALIZED_VIEW_REPAIR",
  SNAPSHOT_SAVE: "SNAPSHOT_SAVE",
});

const DIAGNOSTIC_STATUSES = Object.freeze({
  COMMIT_CONFIRMED_AFTER_ERROR: "COMMIT_CONFIRMED_AFTER_ERROR",
  COMMIT_UNKNOWN: "COMMIT_UNKNOWN",
  LOOKUP_FAILED: "LOOKUP_FAILED",
  REPAIRED: "REPAIRED",
  REPAIR_FAILED: "REPAIR_FAILED",
  SAVE_FAILED: "SAVE_FAILED",
});

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

function createDiagnosticEmitter(reporter = () => {}) {
  if (typeof reporter !== "function") {
    throw new TypeError("diagnosticReporter must be a function");
  }

  return function emitDiagnostic({ type, status, ...context }) {
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
}

module.exports = {
  DIAGNOSTIC_STATUSES,
  DIAGNOSTIC_TYPES,
  createDiagnosticEmitter,
};
