const { createSqliteDatabase } = require("./sqlite/sqliteDatabase");
const { SqliteEventStore } = require("./sqlite/sqliteEventStore");
const { SqliteCommandStore } = require("./sqlite/sqliteCommandStore");
const { SqliteSnapshotStore } = require("./sqlite/sqliteSnapshotStore");
const { SqliteStateRepository } = require("./sqlite/sqliteStateRepository");

const { InMemoryEventStore } = require("./inMemoryEventStore");
const { InMemoryCommandStore } = require("./inMemoryCommandStore");
const { InMemorySnapshotStore } = require("./inMemorySnapshotStore");
const { InMemoryStateRepository } = require("./inMemoryStateRepository");

function createStorageAdapters({
  type = "memory",
  dbPath = ":memory:",
  db = null,
  upcasterRegistry = null,
  now = null,
  wal = true,
  busyTimeout = 5000,
} = {}) {
  if (type === "sqlite") {
    let ownedDb = false;
    let database = db;

    if (!database) {
      database = createSqliteDatabase({
        path: dbPath,
        wal,
        busyTimeout,
      });
      ownedDb = true;
    }

    const eventStore = new SqliteEventStore({ db: database, upcasterRegistry, now });
    const commandStore = new SqliteCommandStore({ db: database });
    const snapshotStore = new SqliteSnapshotStore({ db: database });
    const stateRepository = new SqliteStateRepository({ db: database });

    const close = () => {
      if (ownedDb && database && typeof database.close === "function") {
        database.close();
      }
    };

    return {
      type: "sqlite",
      eventStore,
      commandStore,
      snapshotStore,
      stateRepository,
      db: database,
      close,
    };
  }

  if (type === "memory") {
    const commandStore = new InMemoryCommandStore();
    const eventStore = new InMemoryEventStore({ upcasterRegistry, commandStore, now });
    const snapshotStore = new InMemorySnapshotStore();
    const stateRepository = new InMemoryStateRepository();

    return {
      type: "memory",
      eventStore,
      commandStore,
      snapshotStore,
      stateRepository,
      db: null,
      close: () => {},
    };
  }

  throw new TypeError(`Unsupported storage adapter type: ${type}`);
}

module.exports = {
  createStorageAdapters,
};
