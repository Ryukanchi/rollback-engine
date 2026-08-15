const { isDeepStrictEqual } = require("node:util");

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

  if (!snapshot.state || typeof snapshot.state !== "object" || Array.isArray(snapshot.state)) {
    throw new TypeError("snapshot.state must be an object");
  }

  if (snapshot.state.aggregateId !== snapshot.aggregateId) {
    throw new Error("snapshot.state.aggregateId must match snapshot.aggregateId");
  }

  if (snapshot.state.version !== snapshot.version) {
    throw new Error("snapshot.state.version must match snapshot.version");
  }
}

function cloneSnapshot(snapshot) {
  return structuredClone(snapshot);
}

class InMemorySnapshotStore {
  #snapshots = new Map();

  save(snapshot) {
    assertSnapshot(snapshot);

    const currentSnapshot = this.#snapshots.get(snapshot.aggregateId);

    if (currentSnapshot && snapshot.version < currentSnapshot.version) {
      throw new Error(
        `Cannot replace snapshot version ${currentSnapshot.version} with older version ${snapshot.version} for aggregate ${snapshot.aggregateId}`
      );
    }

    if (
      currentSnapshot &&
      snapshot.version === currentSnapshot.version &&
      !isDeepStrictEqual(snapshot.state, currentSnapshot.state)
    ) {
      throw new Error(
        `Cannot replace snapshot version ${snapshot.version} with different state for aggregate ${snapshot.aggregateId}`
      );
    }

    this.#snapshots.set(snapshot.aggregateId, cloneSnapshot(snapshot));

    return this.getByAggregateId(snapshot.aggregateId);
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);

    const snapshot = this.#snapshots.get(aggregateId);

    return snapshot ? cloneSnapshot(snapshot) : null;
  }
}

module.exports = {
  InMemorySnapshotStore,
};
