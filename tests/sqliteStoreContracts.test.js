const { describe } = require("node:test");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");

const {
  registerEventStoreContract,
  registerCommandStoreContract,
  registerSnapshotStoreContract,
  registerStateRepositoryContract,
} = require("./support/storeContractSuites");

const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");
const { SqliteEventStore } = require("../src/infrastructure/sqlite/sqliteEventStore");
const { SqliteCommandStore } = require("../src/infrastructure/sqlite/sqliteCommandStore");
const { SqliteSnapshotStore } = require("../src/infrastructure/sqlite/sqliteSnapshotStore");
const { SqliteStateRepository } = require("../src/infrastructure/sqlite/sqliteStateRepository");

describe("Sqlite in-memory store contracts", () => {
  registerEventStoreContract({
    adapterName: "SqliteEventStore (memory)",
    createStore: () => {
      const db = createSqliteDatabase({ path: ":memory:" });
      return new SqliteEventStore({ db });
    },
  });

  registerCommandStoreContract({
    adapterName: "SqliteCommandStore (memory)",
    createStore: () => {
      const db = createSqliteDatabase({ path: ":memory:" });
      return new SqliteCommandStore({ db });
    },
  });

  registerSnapshotStoreContract({
    adapterName: "SqliteSnapshotStore (memory)",
    createStore: () => {
      const db = createSqliteDatabase({ path: ":memory:" });
      return new SqliteSnapshotStore({ db });
    },
  });

  registerStateRepositoryContract({
    adapterName: "SqliteStateRepository (memory)",
    createRepository: () => {
      const db = createSqliteDatabase({ path: ":memory:" });
      return new SqliteStateRepository({ db });
    },
  });
});

describe("Sqlite file-backed store contracts", () => {
  function createTempDb() {
    const filePath = join(tmpdir(), `rollback-contract-${randomUUID()}.db`);
    const db = createSqliteDatabase({ path: filePath });
    return { db, filePath };
  }

  registerEventStoreContract({
    adapterName: "SqliteEventStore (file-backed)",
    createStore: () => {
      const { db } = createTempDb();
      return new SqliteEventStore({ db });
    },
  });

  registerCommandStoreContract({
    adapterName: "SqliteCommandStore (file-backed)",
    createStore: () => {
      const { db } = createTempDb();
      return new SqliteCommandStore({ db });
    },
  });

  registerSnapshotStoreContract({
    adapterName: "SqliteSnapshotStore (file-backed)",
    createStore: () => {
      const { db } = createTempDb();
      return new SqliteSnapshotStore({ db });
    },
  });

  registerStateRepositoryContract({
    adapterName: "SqliteStateRepository (file-backed)",
    createRepository: () => {
      const { db } = createTempDb();
      return new SqliteStateRepository({ db });
    },
  });
});
