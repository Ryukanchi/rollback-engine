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

function assertState(state) {
  if (!state || typeof state !== "object" || Array.isArray(state)) {
    throw new TypeError("state must be an object");
  }

  assertAggregateId(state.aggregateId);

  if (!Number.isSafeInteger(state.version) || state.version < 0) {
    throw new TypeError("state.version must be a non-negative safe integer");
  }
}

class SqliteStateRepository {
  #db;

  #stmtGetState;

  #stmtInsertState;

  #stmtUpdateState;

  #stmtAllStates;

  #stmtReset;

  constructor({ db } = {}) {
    if (!db || typeof db.prepare !== "function") {
      throw new TypeError("db must be a valid SQLite database instance");
    }

    this.#db = db;

    this.#stmtGetState = this.#db.prepare(`
      SELECT state FROM materialized_states WHERE aggregate_id = ?
    `);

    this.#stmtInsertState = this.#db.prepare(`
      INSERT INTO materialized_states (aggregate_id, version, state)
      VALUES (?, ?, ?)
    `);

    this.#stmtUpdateState = this.#db.prepare(`
      UPDATE materialized_states SET version = ?, state = ? WHERE aggregate_id = ?
    `);

    this.#stmtAllStates = this.#db.prepare(`
      SELECT state FROM materialized_states ORDER BY rowid ASC
    `);

    this.#stmtReset = this.#db.prepare(`
      DELETE FROM materialized_states
    `);
  }

  save(state) {
    assertState(state);

    const existing = this.#stmtGetState.get(state.aggregateId);

    if (existing) {
      throw new Error(`State for aggregate ${state.aggregateId} already exists`);
    }

    this.#stmtInsertState.run(
      state.aggregateId,
      state.version,
      JSON.stringify(state)
    );

    return this.getByAggregateId(state.aggregateId);
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);
    const row = this.#stmtGetState.get(aggregateId);
    return row ? structuredClone(JSON.parse(row.state)) : null;
  }

  replace(state) {
    assertState(state);

    const existing = this.#stmtGetState.get(state.aggregateId);

    if (!existing) {
      throw new Error(`State for aggregate ${state.aggregateId} does not exist`);
    }

    this.#stmtUpdateState.run(
      state.version,
      JSON.stringify(state),
      state.aggregateId
    );

    return this.getByAggregateId(state.aggregateId);
  }

  getAll() {
    const rows = this.#stmtAllStates.all();
    return rows.map((row) => structuredClone(JSON.parse(row.state)));
  }

  reset() {
    this.#stmtReset.run();
  }
}

module.exports = {
  SqliteStateRepository,
};
