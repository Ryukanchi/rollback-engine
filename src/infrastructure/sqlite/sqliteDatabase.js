const { DatabaseSync } = require("node:sqlite");
const { initializeSchema } = require("./schema");

function createSqliteDatabase({
  path = ":memory:",
  wal = true,
  busyTimeout = 5000,
} = {}) {
  if (typeof path !== "string" || path.trim().length === 0) {
    throw new TypeError("path must be a non-empty string");
  }

  if (!Number.isSafeInteger(busyTimeout) || busyTimeout < 0) {
    throw new TypeError("busyTimeout must be a non-negative integer");
  }

  const db = new DatabaseSync(path);

  db.exec("PRAGMA foreign_keys = ON;");
  db.exec(`PRAGMA busy_timeout = ${busyTimeout};`);

  if (path !== ":memory:" && wal) {
    db.exec("PRAGMA journal_mode = WAL;");
    db.exec("PRAGMA synchronous = NORMAL;");
  }

  initializeSchema(db);

  return db;
}

module.exports = {
  createSqliteDatabase,
};
