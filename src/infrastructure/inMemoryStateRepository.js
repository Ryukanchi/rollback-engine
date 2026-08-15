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
