const { materializedStateIdentity } = require("../application/storeContracts");

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function assertAggregateId(aggregateId) {
  if (!isIdentifier(aggregateId)) {
    throw new TypeError("aggregateId must be a non-empty string or a positive safe integer");
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

function cloneState(state) {
  return structuredClone(state);
}

class InMemoryStateRepository {
  #states = new Map();

  save(state) {
    assertState(state);

    if (this.#states.has(state.aggregateId)) {
      throw new Error(`State for aggregate ${state.aggregateId} already exists`);
    }

    this.#states.set(state.aggregateId, cloneState(state));

    return this.getByAggregateId(state.aggregateId);
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);

    const state = this.#states.get(aggregateId);

    return state ? cloneState(state) : null;
  }

  replace(state) {
    assertState(state);

    if (!this.#states.has(state.aggregateId)) {
      throw new Error(`State for aggregate ${state.aggregateId} does not exist`);
    }

    this.#states.set(state.aggregateId, cloneState(state));

    return this.getByAggregateId(state.aggregateId);
  }

  /**
   * Conditional write: store `nextState` only while the persisted state is
   * still exactly the one the caller observed. `expectedState === null` means
   * the caller observed no row and wants an insert-if-absent.
   *
   * Losing is ordinary concurrency, so it is returned rather than thrown.
   *
   * There is no transaction here and none is claimed: the compare and the write
   * are one uninterrupted synchronous step, which is what makes them atomic in
   * this adapter. The write is delegated to save/replace so that stays the one
   * place a state is actually stored.
   */
  compareAndSwap({ aggregateId, expectedState = null, nextState } = {}) {
    assertAggregateId(aggregateId);
    assertState(nextState);
    assertOwnedBy(aggregateId, nextState, "nextState");

    if (expectedState !== null && expectedState !== undefined) {
      assertState(expectedState);
      assertOwnedBy(aggregateId, expectedState, "expectedState");
    }

    const current = this.#states.get(aggregateId) ?? null;

    if (materializedStateIdentity(current) !== materializedStateIdentity(expectedState ?? null)) {
      return { applied: false };
    }

    current ? this.replace(nextState) : this.save(nextState);

    return { applied: true };
  }

  getAll() {
    return Array.from(this.#states.values(), (state) => cloneState(state));
  }

  reset() {
    this.#states.clear();
  }
}

module.exports = {
  InMemoryStateRepository,
};
