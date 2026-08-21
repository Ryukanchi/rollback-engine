const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { DatabaseSync } = require("node:sqlite");

const { initializeSchema } = require("../src/infrastructure/sqlite/schema");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");

const LEGACY_V1_SCHEMA_SQL = `
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
    error TEXT,
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

test("schema migration: upgrades legacy v1 database to v3 with lease and receipt columns", () => {
  const dbPath = join(tmpdir(), `rollback-migration-v1-to-v3-${randomUUID()}.db`);

  // Step 1: Create a legacy v1 database manually
  const rawDb = new DatabaseSync(dbPath);
  try {
    rawDb.exec(LEGACY_V1_SCHEMA_SQL);
    rawDb.exec("PRAGMA user_version = 1;");

    // Insert legacy data into v1 commands
    rawDb.prepare(`
      INSERT INTO commands (command_id, command_type, payload, status, result)
      VALUES (?, ?, ?, ?, ?)
    `).run(
      "legacy-cmd-1",
      "CHECKOUT",
      JSON.stringify({ item: "Book", quantity: 1, amount: 20 }),
      "completed",
      JSON.stringify({ status: "completed", aggregateId: 1 })
    );

    // Verify v1 columns do not have lease_owner yet
    const colsV1 = rawDb.prepare("PRAGMA table_info(commands);").all().map((c) => c.name);
    assert.equal(colsV1.includes("lease_owner"), false);
    assert.equal(colsV1.includes("lease_token"), false);
    assert.equal(colsV1.includes("lease_expires_at"), false);
  } finally {
    rawDb.close();
  }

  // Step 2: Open database with standard adapters (runs initializeSchema migration)
  // The reservation deadline below is the property under test, so the store
  // that computes it is given the clock that fixes it.
  const adapters = createStorageAdapters({
    type: "sqlite",
    dbPath,
    leaseNow: () => 10000,
  });

  try {
    // Verify user_version is now 3
    const versionRow = adapters.db.prepare("PRAGMA user_version;").get();
    const version = Object.values(versionRow)[0];
    assert.equal(version, 3);

    // Verify columns exist now
    const colsV3 = adapters.db.prepare("PRAGMA table_info(commands);").all().map((c) => c.name);
    assert.equal(colsV3.includes("lease_owner"), true);
    assert.equal(colsV3.includes("lease_token"), true);
    assert.equal(colsV3.includes("lease_expires_at"), true);
    assert.equal(colsV3.includes("receipt_metadata"), true);

    // Verify legacy record is intact and accessible
    const legacyCmd = adapters.commandStore.get("legacy-cmd-1");
    assert.equal(legacyCmd.commandId, "legacy-cmd-1");
    assert.equal(legacyCmd.status, "completed");
    assert.equal(legacyCmd.leaseToken, 1);
    assert.equal(legacyCmd.leaseOwner, null);
    assert.equal(legacyCmd.leaseExpiresAt, null);
    assert.equal(legacyCmd.receiptMetadata, null);

    // Verify new commands can be reserved with leases
    const newReservation = adapters.commandStore.reserve({
      commandId: "new-cmd-v2",
      commandType: "CHECKOUT",
      payload: { item: "Pen", quantity: 2, amount: 10 },
      workerId: "worker-migrated",
      leaseTtlMs: 5000,
    });
    assert.equal(newReservation.created, true);
    assert.equal(newReservation.record.leaseOwner, "worker-migrated");
    assert.equal(newReservation.record.leaseToken, 1);
    assert.equal(newReservation.record.leaseExpiresAt, 15000);
  } finally {
    adapters.close();
  }
});
