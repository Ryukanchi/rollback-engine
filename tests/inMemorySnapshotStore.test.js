const test = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const { projectEvents } = require("../src/domain/projection");
const {
  InMemorySnapshotStore,
} = require("../src/infrastructure/inMemorySnapshotStore");

function event(eventType, aggregateId, sequence, timestamp, payload) {
  return createDomainEvent({
    eventId: `event-${aggregateId}-${sequence}`,
    eventType,
    aggregateId,
    sequence,
    timestamp,
    payload,
  });
}

function stateAtVersionOne(aggregateId, item = "Pizza") {
  return projectEvents([
    event(
      EVENT_TYPES.ORDER_CREATED,
      aggregateId,
      1,
      "2026-08-14T10:00:01.000Z",
      { item, quantity: 1 }
    ),
  ]);
}

function stateAtVersionTwo(aggregateId) {
  return projectEvents([
    event(
      EVENT_TYPES.ORDER_CREATED,
      aggregateId,
      1,
      "2026-08-14T10:00:01.000Z",
      { item: "Pizza", quantity: 1 }
    ),
    event(
      EVENT_TYPES.INVENTORY_RESERVED,
      aggregateId,
      2,
      "2026-08-14T10:00:02.000Z",
      { reservationId: 10, item: "Pizza", quantity: 1 }
    ),
  ]);
}

function snapshotFor(state, timestamp) {
  return {
    aggregateId: state.aggregateId,
    version: state.version,
    timestamp,
    state,
  };
}

test("saves and loads snapshots by aggregate ID", () => {
  const store = new InMemorySnapshotStore();
  const state = stateAtVersionOne(1);
  const snapshot = snapshotFor(state, "2026-08-14T10:00:01.000Z");

  assert.deepEqual(store.save(snapshot), snapshot);
  assert.deepEqual(store.getByAggregateId(1), snapshot);
  assert.equal(store.getByAggregateId(999), null);
});

test("keeps snapshots for different aggregates independent", () => {
  const store = new InMemorySnapshotStore();
  const firstSnapshot = snapshotFor(
    stateAtVersionOne(1, "Pizza"),
    "2026-08-14T10:00:01.000Z"
  );
  const secondSnapshot = snapshotFor(
    stateAtVersionOne(2, "Pasta"),
    "2026-08-14T10:00:01.000Z"
  );

  store.save(firstSnapshot);
  store.save(secondSnapshot);

  assert.deepEqual(store.getByAggregateId(1), firstSnapshot);
  assert.deepEqual(store.getByAggregateId(2), secondSnapshot);
});

test("replaces an aggregate snapshot with a newer version and rejects stale versions", () => {
  const store = new InMemorySnapshotStore();
  const versionOne = snapshotFor(
    stateAtVersionOne(1),
    "2026-08-14T10:00:01.000Z"
  );
  const versionTwo = snapshotFor(
    stateAtVersionTwo(1),
    "2026-08-14T10:00:02.000Z"
  );

  store.save(versionOne);
  store.save(versionTwo);

  assert.deepEqual(store.getByAggregateId(1), versionTwo);
  assert.throws(() => store.save(versionOne), /Cannot replace snapshot version 2 with older version 1/);
});

test("isolates stored snapshots from caller mutations", () => {
  const store = new InMemorySnapshotStore();
  const state = stateAtVersionOne(1);
  const snapshot = snapshotFor(state, "2026-08-14T10:00:01.000Z");

  store.save(snapshot);
  snapshot.state.order.item = "Changed input";

  const loadedSnapshot = store.getByAggregateId(1);
  loadedSnapshot.state.order.item = "Changed loaded copy";

  assert.equal(store.getByAggregateId(1).state.order.item, "Pizza");
});

test("rejects snapshots whose state does not match aggregate or version", () => {
  const store = new InMemorySnapshotStore();
  const state = stateAtVersionOne(1);

  assert.throws(
    () =>
      store.save({
        aggregateId: 2,
        version: state.version,
        timestamp: "2026-08-14T10:00:01.000Z",
        state,
      }),
    /snapshot.state.aggregateId must match/
  );

  assert.throws(
    () =>
      store.save({
        aggregateId: 1,
        version: 2,
        timestamp: "2026-08-14T10:00:01.000Z",
        state,
      }),
    /snapshot.state.version must match/
  );
});

test("rejects a conflicting replacement at the same snapshot version", () => {
  const store = new InMemorySnapshotStore();
  const original = snapshotFor(
    stateAtVersionOne(1, "Pizza"),
    "2026-08-14T10:00:01.000Z"
  );
  const conflicting = snapshotFor(
    stateAtVersionOne(1, "Pasta"),
    "2026-08-14T10:00:01.000Z"
  );

  store.save(original);

  assert.throws(
    () => store.save(conflicting),
    /Cannot replace snapshot version 1 with different state/
  );
  assert.deepEqual(store.getByAggregateId(1), original);
});
