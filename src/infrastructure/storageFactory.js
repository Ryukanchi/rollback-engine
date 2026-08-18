const { createSqliteDatabase } = require("./sqlite/sqliteDatabase");
const { SqliteEventStore } = require("./sqlite/sqliteEventStore");
const { SqliteCommandStore } = require("./sqlite/sqliteCommandStore");
const { SqliteSnapshotStore } = require("./sqlite/sqliteSnapshotStore");
const { SqliteStateRepository } = require("./sqlite/sqliteStateRepository");

const { InMemoryEventStore } = require("./inMemoryEventStore");
const { InMemoryCommandStore } = require("./inMemoryCommandStore");
const { InMemorySnapshotStore } = require("./inMemorySnapshotStore");
const { InMemoryStateRepository } = require("./inMemoryStateRepository");

/**
 * `leaseNow` is the Command Store's lease clock: it decides when a lease is
 * created, extended, transferable or revocable. It is deliberately a separate
 * option from `now`, which is the Event Store clock - the two answer different
 * questions and must never be conflated.
 */
function createStorageAdapters({
  type = "memory",
  dbPath = ":memory:",
  db = null,
  upcasterRegistry = null,
  now = null,
  leaseNow = null,
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
    const commandStore = new SqliteCommandStore({ db: database, now: leaseNow ?? undefined });
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
    const commandStore = new InMemoryCommandStore({ now: leaseNow ?? undefined });
    const eventStore = new InMemoryEventStore({ upcasterRegistry, commandStore, now });
    commandStore.setEventStore(eventStore);
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
