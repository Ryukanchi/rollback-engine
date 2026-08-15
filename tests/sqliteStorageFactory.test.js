const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { SqliteEventStore } = require("../src/infrastructure/sqlite/sqliteEventStore");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");

test("createStorageAdapters creates memory adapters by default", () => {
  const adapters = createStorageAdapters({ type: "memory" });
  assert.equal(adapters.type, "memory");
  assert.equal(adapters.eventStore instanceof InMemoryEventStore, true);
  assert.equal(adapters.db, null);
  assert.doesNotThrow(() => adapters.close());
});

test("createStorageAdapters creates sqlite adapters in memory", () => {
  const adapters = createStorageAdapters({ type: "sqlite", dbPath: ":memory:" });
  assert.equal(adapters.type, "sqlite");
  assert.equal(adapters.eventStore instanceof SqliteEventStore, true);
  assert.notEqual(adapters.db, null);
  assert.doesNotThrow(() => adapters.close());
});

test("createStorageAdapters creates sqlite adapters file-backed", () => {
  const dbPath = join(tmpdir(), `rollback-factory-${randomUUID()}.db`);
  const adapters = createStorageAdapters({ type: "sqlite", dbPath });
  assert.equal(adapters.type, "sqlite");
  assert.equal(adapters.eventStore instanceof SqliteEventStore, true);
  assert.notEqual(adapters.db, null);
  assert.doesNotThrow(() => adapters.close());
});

test("createStorageAdapters rejects unsupported storage types", () => {
  assert.throws(() => createStorageAdapters({ type: "postgres" }), TypeError);
});
