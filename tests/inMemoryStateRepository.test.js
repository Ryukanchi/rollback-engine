const test = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const { applyEvent, createInitialState } = require("../src/domain/projection");
const {
  InMemoryStateRepository,
} = require("../src/infrastructure/inMemoryStateRepository");

function createdState(aggregateId, item) {
  return applyEvent(
    createInitialState(aggregateId),
    createDomainEvent({
      eventId: `order-${aggregateId}-created`,
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId,
      sequence: 1,
      timestamp: "2026-08-14T10:00:00.000Z",
      payload: { item, quantity: 1 },
    })
  );
}

test("saves, reads, replaces and lists aggregate states", () => {
  const repository = new InMemoryStateRepository();
  const initialState = createInitialState(1);
  const aggregateTwo = createdState(2, "Pasta");

  assert.deepEqual(repository.save(initialState), initialState);
  assert.deepEqual(repository.save(aggregateTwo), aggregateTwo);
  assert.deepEqual(repository.getAll(), [initialState, aggregateTwo]);

  const replacement = createdState(1, "Pizza");

  assert.deepEqual(repository.replace(replacement), replacement);
  assert.deepEqual(repository.getByAggregateId(1), replacement);
  assert.deepEqual(repository.getAll(), [replacement, aggregateTwo]);
});

test("isolates stored state from caller mutations", () => {
  const repository = new InMemoryStateRepository();
  const state = createdState(1, "Pizza");

  repository.save(state);
  state.order.item = "Changed input";

  const loadedState = repository.getByAggregateId(1);
  loadedState.order.item = "Changed loaded copy";

  assert.equal(repository.getByAggregateId(1).order.item, "Pizza");
});

test("rejects duplicate saves and replacements for missing aggregates", () => {
  const repository = new InMemoryStateRepository();
  const state = createInitialState(1);

  repository.save(state);

  assert.throws(() => repository.save(state), /already exists/);
  assert.throws(() => repository.replace(createInitialState(2)), /does not exist/);
  assert.equal(repository.getByAggregateId(999), null);
});

test("reset clears all states for isolated tests", () => {
  const repository = new InMemoryStateRepository();

  repository.save(createInitialState(1));
  repository.save(createInitialState(2));
  repository.reset();

  assert.deepEqual(repository.getAll(), []);
  assert.equal(repository.getByAggregateId(1), null);
  assert.equal(repository.getByAggregateId(2), null);
});
