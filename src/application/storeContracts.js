const COMMAND_STATUSES = Object.freeze({
  PROCESSING: "processing",
  COMPLETED: "completed",
  FAILED: "failed",
  RELEASED: "released",
});

const STORE_ADAPTER_METHODS = Object.freeze({
  eventStore: Object.freeze([
    "append",
    "getByAggregateId",
    "getByAggregateIdAfter",
    "getByCommandId",
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
    "getByAggregateId",
    "getAll",
  ]),
});

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
  STORE_ADAPTER_METHODS,
  assertLeaseTtlMs,
  createLeaseDeadline,
  assertCommandStoreAdapter,
  assertEventStoreAdapter,
  assertSnapshotStoreAdapter,
  assertStateRepositoryAdapter,
};
