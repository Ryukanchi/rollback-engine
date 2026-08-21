const COMMAND_STATUSES = Object.freeze({
  PROCESSING: "processing",
  COMPLETED: "completed",
  FAILED: "failed",
  RELEASED: "released",
});

const CURRENT_COMMAND_RECEIPT_CONTRACT_VERSION = 1;

const COMMAND_RECEIPT_DOMAIN_EFFECTS = Object.freeze({
  EVENTS: "events",
  NONE: "none",
});

const STORE_ADAPTER_METHODS = Object.freeze({
  eventStore: Object.freeze([
    "append",
    "getByAggregateId",
    "getByAggregateIdAfter",
    "getByCommandId",
    "getRawByCommandIdForReconciliation",
    "getAll",
    "getLastSequence",
  ]),
  commandStore: Object.freeze([
    "reserve",
    "recordEvent",
    "complete",
    "fail",
    "release",
    "releaseFailed",
    "get",
    "reconcileEvents",
    "reconcileFailure",
    "takeOverExpired",
    "revokeExpired",
    "renewLease",
  ]),
  snapshotStore: Object.freeze(["save", "getByAggregateId"]),
  stateRepository: Object.freeze([
    "save",
    "replace",
    "compareAndSwap",
    "getByAggregateId",
    "getAll",
  ]),
});

/**
 * The identity a materialized-view write is conditioned on.
 *
 * This is deliberately not a claim that JSON.stringify is a canonical encoding.
 * The narrower contract that holds here is enough: a projection state written
 * by a state repository and read back through it re-serialises to the same
 * text, so comparing this representation answers exactly one question - "is the
 * stored view still the one this writer observed?". Version is never consulted,
 * because authoritative repair has to stay free to move the view backwards.
 */
function materializedStateIdentity(state) {
  return state === null || state === undefined ? null : JSON.stringify(state);
}

/**
 * A lease duration is caller-supplied execution policy, and deliberately stays
 * that way: unlike the lease clock, a worker choosing its own lease length can
 * only shorten its own protection or delay its own recovery. It can never buy
 * authority it does not hold, because authority is status plus generation.
 *
 * It must still be a real duration. A malformed one is not caught anywhere
 * downstream - it is simply added to the clock, and the resulting value means
 * different things in each adapter.
 */
function assertLeaseTtlMs(leaseTtlMs) {
  if (!Number.isSafeInteger(leaseTtlMs) || leaseTtlMs <= 0) {
    throw new TypeError(
      `leaseTtlMs must be a positive safe integer, got ${String(leaseTtlMs)}`
    );
  }
}

/**
 * Builds the only lease deadline a command store is allowed to persist.
 *
 * Validating the duration alone is not enough. `now + leaseTtlMs` can leave the
 * safe integer range while both operands still look reasonable, and SQLite
 * accepts that write and then cannot read the row back at all - every later
 * operation on the command throws, so it can never be completed, revoked or
 * taken over. The same check covers a store clock that cannot produce a usable
 * instant, because a deadline has two operands and only one of them is policy.
 */
function createLeaseDeadline(now, leaseTtlMs) {
  assertLeaseTtlMs(leaseTtlMs);

  const leaseExpiresAt = now + leaseTtlMs;

  if (!Number.isSafeInteger(leaseExpiresAt) || !(leaseExpiresAt > now)) {
    throw new TypeError(
      `lease deadline must be a safe integer later than the store clock reading, got ${String(
        leaseExpiresAt
      )} from clock ${String(now)}`
    );
  }

  return leaseExpiresAt;
}

/**
 * Validates only the persisted receipt shape introduced by F-12 phase 1.
 * Whether an anchor still agrees with Event History is deliberately outside
 * this Store contract and belongs to the later completed-receipt read boundary.
 */
function assertCommandReceiptMetadata(receiptMetadata) {
  if (
    !receiptMetadata ||
    typeof receiptMetadata !== "object" ||
    Array.isArray(receiptMetadata)
  ) {
    throw new TypeError("receiptMetadata must be an object");
  }

  if (
    receiptMetadata.contractVersion !==
    CURRENT_COMMAND_RECEIPT_CONTRACT_VERSION
  ) {
    throw new TypeError(
      `receiptMetadata.contractVersion must be ${CURRENT_COMMAND_RECEIPT_CONTRACT_VERSION}`
    );
  }

  if (
    !Object.values(COMMAND_RECEIPT_DOMAIN_EFFECTS).includes(
      receiptMetadata.domainEffect
    )
  ) {
    throw new TypeError(
      'receiptMetadata.domainEffect must be "events" or "none"'
    );
  }

  const { stateAnchor } = receiptMetadata;

  if (stateAnchor === null) {
    return receiptMetadata;
  }

  if (!stateAnchor || typeof stateAnchor !== "object" || Array.isArray(stateAnchor)) {
    throw new TypeError("receiptMetadata.stateAnchor must be an object or null");
  }

  if (
    !(
      (typeof stateAnchor.aggregateId === "string" &&
        stateAnchor.aggregateId.trim().length > 0) ||
      (Number.isSafeInteger(stateAnchor.aggregateId) && stateAnchor.aggregateId > 0)
    )
  ) {
    throw new TypeError(
      "receiptMetadata.stateAnchor.aggregateId must be a non-empty string or a positive safe integer"
    );
  }

  if (!Number.isSafeInteger(stateAnchor.sequence) || stateAnchor.sequence < 0) {
    throw new TypeError(
      "receiptMetadata.stateAnchor.sequence must be a non-negative safe integer"
    );
  }

  if (stateAnchor.sequence === 0) {
    if (stateAnchor.lastEventId !== null) {
      throw new TypeError(
        "receiptMetadata.stateAnchor.lastEventId must be null at sequence 0"
      );
    }
  } else if (
    typeof stateAnchor.lastEventId !== "string" ||
    stateAnchor.lastEventId.trim().length === 0
  ) {
    throw new TypeError(
      "receiptMetadata.stateAnchor.lastEventId must be a non-empty string after sequence 0"
    );
  }

  return receiptMetadata;
}

function assertStoreAdapter(adapter, adapterName, requiredMethods) {
  if (
    !adapter ||
    (typeof adapter !== "object" && typeof adapter !== "function")
  ) {
    throw new TypeError(`${adapterName} must be an object`);
  }

  for (const methodName of requiredMethods) {
    if (typeof adapter[methodName] !== "function") {
      throw new TypeError(`${adapterName} must implement ${methodName}()`);
    }
  }

  return adapter;
}

function assertEventStoreAdapter(adapter) {
  return assertStoreAdapter(
    adapter,
    "eventStore",
    STORE_ADAPTER_METHODS.eventStore
  );
}

function assertCommandStoreAdapter(adapter) {
  return assertStoreAdapter(
    adapter,
    "commandStore",
    STORE_ADAPTER_METHODS.commandStore
  );
}

function assertSnapshotStoreAdapter(adapter) {
  return assertStoreAdapter(
    adapter,
    "snapshotStore",
    STORE_ADAPTER_METHODS.snapshotStore
  );
}

function assertStateRepositoryAdapter(adapter) {
  return assertStoreAdapter(
    adapter,
    "stateRepository",
    STORE_ADAPTER_METHODS.stateRepository
  );
}

module.exports = {
  COMMAND_STATUSES,
  COMMAND_RECEIPT_DOMAIN_EFFECTS,
  CURRENT_COMMAND_RECEIPT_CONTRACT_VERSION,
  STORE_ADAPTER_METHODS,
  assertCommandReceiptMetadata,
  assertLeaseTtlMs,
  createLeaseDeadline,
  materializedStateIdentity,
  assertCommandStoreAdapter,
  assertEventStoreAdapter,
  assertSnapshotStoreAdapter,
  assertStateRepositoryAdapter,
};
