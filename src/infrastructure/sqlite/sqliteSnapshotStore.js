const { isDeepStrictEqual } = require("node:util");

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function assertAggregateId(aggregateId) {
  if (!isIdentifier(aggregateId)) {
    throw new TypeError(
      "aggregateId must be a non-empty string or a positive safe integer"
    );
  }
}

function assertNormalizedTimestamp(timestamp) {
  if (typeof timestamp !== "string" || timestamp.length === 0) {
    throw new TypeError("snapshot.timestamp must be a valid date string");
  }

  const parsedTimestamp = new Date(timestamp);

  if (Number.isNaN(parsedTimestamp.getTime())) {
    throw new TypeError("snapshot.timestamp must be a valid date string");
  }

  if (parsedTimestamp.toISOString() !== timestamp) {
    throw new TypeError("snapshot.timestamp must use normalized ISO-8601 UTC format");
  }
}

function assertSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) {
    throw new TypeError("snapshot must be an object");
  }

  assertAggregateId(snapshot.aggregateId);

  if (!Number.isSafeInteger(snapshot.version) || snapshot.version <= 0) {
    throw new TypeError("snapshot.version must be a positive safe integer");
  }

  assertNormalizedTimestamp(snapshot.timestamp);

  if (
    snapshot.lastEventId !== undefined &&
    (typeof snapshot.lastEventId !== "string" ||
      snapshot.lastEventId.trim().length === 0)
  ) {
    throw new TypeError("snapshot.lastEventId must be a non-empty string");
  }

  if (
    !snapshot.state ||
    typeof snapshot.state !== "object" ||
    Array.isArray(snapshot.state)
  ) {
    throw new TypeError("snapshot.state must be an object");
  }

  if (snapshot.state.aggregateId !== snapshot.aggregateId) {
    throw new Error("snapshot.state.aggregateId must match snapshot.aggregateId");
  }

  if (snapshot.state.version !== snapshot.version) {
    throw new Error("snapshot.state.version must match snapshot.version");
  }
}

function rowToSnapshot(row) {
  if (!row) {
    return null;
  }

  const snapshot = {
    aggregateId: row.aggregate_id,
    version: Number(row.version),
    timestamp: row.timestamp,
    state: JSON.parse(row.state),
  };

  if (row.last_event_id !== null && row.last_event_id !== undefined) {
    snapshot.lastEventId = row.last_event_id;
  }

  return structuredClone(snapshot);
}

class SqliteSnapshotStore {
  #db;

  #stmtGetSnapshot;

  #stmtUpsertSnapshot;

  constructor({ db } = {}) {
    if (!db || typeof db.prepare !== "function") {
      throw new TypeError("db must be a valid SQLite database instance");
    }

    this.#db = db;

    this.#stmtGetSnapshot = this.#db.prepare(`
      SELECT aggregate_id, version, timestamp, last_event_id, state
      FROM snapshots
      WHERE aggregate_id = ?
    `);

    this.#stmtUpsertSnapshot = this.#db.prepare(`
      INSERT INTO snapshots (aggregate_id, version, timestamp, last_event_id, state)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(aggregate_id) DO UPDATE SET
        version = excluded.version,
        timestamp = excluded.timestamp,
        last_event_id = excluded.last_event_id,
        state = excluded.state
    `);
  }

  save(snapshot) {
    assertSnapshot(snapshot);

    const currentRow = this.#stmtGetSnapshot.get(snapshot.aggregateId);

    if (currentRow) {
      const currentSnapshot = rowToSnapshot(currentRow);

      if (snapshot.version < currentSnapshot.version) {
        throw new Error(
          `Cannot replace snapshot version ${currentSnapshot.version} with older version ${snapshot.version} for aggregate ${snapshot.aggregateId}`
        );
      }

      if (
        snapshot.version === currentSnapshot.version &&
        !isDeepStrictEqual(snapshot.state, currentSnapshot.state)
      ) {
        throw new Error(
          `Cannot replace snapshot version ${snapshot.version} with different state for aggregate ${snapshot.aggregateId}`
        );
      }
    }

    this.#stmtUpsertSnapshot.run(
      snapshot.aggregateId,
      snapshot.version,
      snapshot.timestamp,
      snapshot.lastEventId ?? null,
      JSON.stringify(snapshot.state)
    );

    return this.getByAggregateId(snapshot.aggregateId);
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);
    const row = this.#stmtGetSnapshot.get(aggregateId);
    return rowToSnapshot(row);
  }
}

module.exports = {
  SqliteSnapshotStore,
};
