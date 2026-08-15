const COMMAND_STATUSES = Object.freeze({
  PROCESSING: "processing",
  COMPLETED: "completed",
  FAILED: "failed",
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
  ]),
  snapshotStore: Object.freeze(["save", "getByAggregateId"]),
  stateRepository: Object.freeze([
    "save",
    "replace",
    "getByAggregateId",
    "getAll",
  ]),
});

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
  assertCommandStoreAdapter,
  assertEventStoreAdapter,
  assertSnapshotStoreAdapter,
  assertStateRepositoryAdapter,
};
