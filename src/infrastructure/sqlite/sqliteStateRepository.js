const { materializedStateIdentity } = require("../../application/storeContracts");

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

function assertOwnedBy(aggregateId, state, fieldName) {
  if (state.aggregateId !== aggregateId) {
    throw new TypeError(`${fieldName} must belong to aggregate ${aggregateId}`);
  }
}

class SqliteStateRepository {
  #db;

  #stmtCompareAndSwap;

  #stmtInsertIfAbsent;

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

    // One statement decides the winner. A SELECT followed by an unconditional
    // UPDATE would put the decision before the write and reopen the very gap
    // this exists to close. `version` is redundant with the version inside the
    // serialised state and is matched anyway, so the predicate covers the whole
    // persisted row rather than one column of it.
    this.#stmtCompareAndSwap = this.#db.prepare(`
      UPDATE materialized_states
      SET version = ?, state = ?
      WHERE aggregate_id = ? AND version = ? AND state = ?
    `);

    this.#stmtInsertIfAbsent = this.#db.prepare(`
      INSERT INTO materialized_states (aggregate_id, version, state)
      VALUES (?, ?, ?)
      ON CONFLICT DO NOTHING
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

  /**
   * Conditional write: store `nextState` only while the persisted row is still
   * exactly the one the caller observed. `expectedState === null` means the
   * caller observed no row and wants an insert-if-absent, which the aggregate
   * primary key decides atomically.
   *
   * Losing is ordinary concurrency, so it is returned rather than thrown.
   */
  compareAndSwap({ aggregateId, expectedState = null, nextState } = {}) {
    assertAggregateId(aggregateId);
    assertState(nextState);
    assertOwnedBy(aggregateId, nextState, "nextState");

    if (expectedState === null || expectedState === undefined) {
      const inserted = this.#stmtInsertIfAbsent.run(
        aggregateId,
        nextState.version,
        materializedStateIdentity(nextState)
      );

      return { applied: inserted.changes === 1 };
    }

    assertState(expectedState);
    assertOwnedBy(aggregateId, expectedState, "expectedState");

    const applied = this.#stmtCompareAndSwap.run(
      nextState.version,
      materializedStateIdentity(nextState),
      aggregateId,
      expectedState.version,
      materializedStateIdentity(expectedState)
    );

    return { applied: applied.changes === 1 };
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
