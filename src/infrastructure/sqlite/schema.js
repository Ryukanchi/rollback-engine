const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY NOT NULL,
    aggregate_id ANY NOT NULL,
    sequence INTEGER NOT NULL,
    command_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    payload TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (aggregate_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_command_id ON events (command_id);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_seq ON events (aggregate_id, sequence);

CREATE TABLE IF NOT EXISTS commands (
    command_id TEXT PRIMARY KEY NOT NULL,
    command_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    aggregate_id ANY,
    event_range TEXT,
    result TEXT,
    receipt_metadata TEXT,
    error TEXT,
    lease_owner TEXT,
    lease_token INTEGER NOT NULL DEFAULT 1,
    lease_expires_at INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_commands_status ON commands (status);

CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id ANY PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    last_event_id TEXT,
    state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS materialized_states (
    aggregate_id ANY PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL
);
`;

function initializeSchema(db) {
  if (!db || typeof db.exec !== "function") {
    throw new TypeError("db must be a valid SQLite database instance");
  }

  // Ensure base tables exist
  db.exec(SCHEMA_SQL);

  // Check version and run migrations if necessary
  const userVerRow = db.prepare("PRAGMA user_version;").get();
  const currentVersion = userVerRow ? Object.values(userVerRow)[0] : 0;

  if (currentVersion < 2) {
    const tableInfo = db.prepare("PRAGMA table_info(commands);").all();
    const cols = new Set(tableInfo.map((c) => c.name));

    if (!cols.has("lease_owner")) {
      db.exec("ALTER TABLE commands ADD COLUMN lease_owner TEXT;");
    }
    if (!cols.has("lease_token")) {
      db.exec("ALTER TABLE commands ADD COLUMN lease_token INTEGER NOT NULL DEFAULT 1;");
    }
    if (!cols.has("lease_expires_at")) {
      db.exec("ALTER TABLE commands ADD COLUMN lease_expires_at INTEGER;");
    }

    db.exec("PRAGMA user_version = 2;");
  }

  db.exec("CREATE INDEX IF NOT EXISTS idx_commands_lease ON commands (status, lease_expires_at);");

  if (currentVersion < 3) {
    const tableInfo = db.prepare("PRAGMA table_info(commands);").all();
    const cols = new Set(tableInfo.map((c) => c.name));

    if (!cols.has("receipt_metadata")) {
      db.exec("ALTER TABLE commands ADD COLUMN receipt_metadata TEXT;");
    }

    db.exec("PRAGMA user_version = 3;");
  }
}

module.exports = {
  SCHEMA_SQL,
  initializeSchema,
};
