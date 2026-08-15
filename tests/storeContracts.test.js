const test = require("node:test");
const assert = require("node:assert/strict");

const {
  STORE_ADAPTER_METHODS,
  assertCommandStoreAdapter,
  assertEventStoreAdapter,
  assertSnapshotStoreAdapter,
  assertStateRepositoryAdapter,
} = require("../src/application/storeContracts");

function adapterWith(methodNames) {
  return Object.fromEntries(
    methodNames.map((methodName) => [methodName, () => {}])
  );
}

test("accepts adapters that expose every required store capability", () => {
  assert.doesNotThrow(() =>
    assertEventStoreAdapter(adapterWith(STORE_ADAPTER_METHODS.eventStore))
  );
  assert.doesNotThrow(() =>
    assertCommandStoreAdapter(adapterWith(STORE_ADAPTER_METHODS.commandStore))
  );
  assert.doesNotThrow(() =>
    assertSnapshotStoreAdapter(adapterWith(STORE_ADAPTER_METHODS.snapshotStore))
  );
  assert.doesNotThrow(() =>
    assertStateRepositoryAdapter(
      adapterWith(STORE_ADAPTER_METHODS.stateRepository)
    )
  );
});

test("rejects incomplete adapters at the composition boundary", () => {
  assert.throws(
    () => assertEventStoreAdapter({}),
    /eventStore must implement append\(\)/
  );
  assert.throws(
    () => assertCommandStoreAdapter({ reserve() {} }),
    /commandStore must implement recordEvent\(\)/
  );
  assert.throws(
    () => assertSnapshotStoreAdapter({ save() {} }),
    /snapshotStore must implement getByAggregateId\(\)/
  );
  assert.throws(
    () => assertStateRepositoryAdapter({ save() {} }),
    /stateRepository must implement replace\(\)/
  );
});

test("keeps in-memory-only reset outside the State Repository contract", () => {
  assert.equal(STORE_ADAPTER_METHODS.stateRepository.includes("reset"), false);
});
