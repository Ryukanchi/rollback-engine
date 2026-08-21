const test = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const {
  InMemoryCommandStore,
} = require("../src/infrastructure/inMemoryCommandStore");
const {
  commandReceiptMetadata,
} = require("./support/commandReceiptFixtures");

function createEvent() {
  return createDomainEvent({
    eventId: "event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 7,
    sequence: 1,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: "Pizza", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId: "checkout-command-1",
      correlationId: "checkout-command-1",
      causationId: "checkout-command-1",
    },
  });
}

test("reserves, tracks and completes an idempotent command", () => {
  const store = new InMemoryCommandStore();
  const descriptor = {
    commandId: "checkout-command-1",
    commandType: "CHECKOUT",
    payload: { item: "Pizza", quantity: 1, amount: 100 },
  };

  const reservation = store.reserve(descriptor);
  store.recordEvent(descriptor.commandId, createEvent(), { fencingToken: 1 });
  store.complete(
    descriptor.commandId,
    { aggregateId: 7, status: "completed" },
    {
      fencingToken: 1,
      receiptMetadata: commandReceiptMetadata({ domainEffect: "events" }),
    }
  );

  const stored = store.get(descriptor.commandId);

  assert.equal(reservation.created, true);
  assert.equal(stored.status, "completed");
  assert.deepEqual(stored.eventRange, {
    aggregateId: 7,
    firstSequence: 1,
    lastSequence: 1,
    eventIds: ["event-1"],
  });
  assert.deepEqual(stored.result, {
    aggregateId: 7,
    status: "completed",
  });
});

test("recognizes the same command and rejects a different payload for the key", () => {
  const store = new InMemoryCommandStore();
  const descriptor = {
    commandId: "checkout-command-1",
    commandType: "CHECKOUT",
    payload: { item: "Pizza", quantity: 1, amount: 100 },
  };

  store.reserve(descriptor);

  const repeated = store.reserve({
    commandId: "checkout-command-1",
    commandType: "CHECKOUT",
    payload: { amount: 100, quantity: 1, item: "Pizza" },
  });
  const conflicting = store.reserve({
    ...descriptor,
    payload: { item: "Pasta", quantity: 1, amount: 100 },
  });
  const differentCommandType = store.reserve({
    commandId: "checkout-command-1",
    commandType: "CREATE_ORDER",
    payload: { item: "Pizza", quantity: 1, amount: 100 },
  });

  assert.equal(repeated.created, false);
  assert.equal(repeated.conflict, false);
  assert.equal(conflicting.created, false);
  assert.equal(conflicting.conflict, true);
  assert.equal(differentCommandType.created, false);
  assert.equal(differentCommandType.conflict, true);
});

test("command records are isolated from caller mutation", () => {
  const store = new InMemoryCommandStore();
  const payload = { item: "Pizza", quantity: 1, amount: 100 };
  const result = { aggregateId: 7, state: { lifecycle: "completed" } };

  store.reserve({
    commandId: "checkout-command-1",
    commandType: "CHECKOUT",
    payload,
  });
  store.complete("checkout-command-1", result, {
    fencingToken: 1,
    receiptMetadata: commandReceiptMetadata(),
  });

  payload.item = "Changed";
  result.state.lifecycle = "Changed";
  const firstRead = store.get("checkout-command-1");
  firstRead.result.state.lifecycle = "Changed again";

  assert.equal(store.get("checkout-command-1").payload.item, "Pizza");
  assert.equal(
    store.get("checkout-command-1").result.state.lifecycle,
    "completed"
  );
});

test("failed defensive copies leave command transitions untouched", () => {
  const store = new InMemoryCommandStore();

  store.reserve({
    commandId: "complete-clone-failure",
    commandType: "TEST",
    payload: { value: 1 },
  });
  store.reserve({
    commandId: "fail-clone-failure",
    commandType: "TEST",
    payload: { value: 2 },
  });

  assert.throws(
    () =>
      store.complete(
        "complete-clone-failure",
        { uncloneable: () => {} },
        { fencingToken: 1, receiptMetadata: commandReceiptMetadata() }
      ),
    { name: "DataCloneError" }
  );
  assert.throws(
    () =>
      store.fail(
        "fail-clone-failure",
        { uncloneable: () => {} },
        { fencingToken: 1 }
      ),
    { name: "DataCloneError" }
  );
  assert.equal(store.get("complete-clone-failure").status, "processing");
  assert.equal(store.get("fail-clone-failure").status, "processing");
});

test("rejects events that identify a different command", () => {
  const store = new InMemoryCommandStore();

  store.reserve({
    commandId: "another-command",
    commandType: "CHECKOUT",
    payload: { item: "Pizza", quantity: 1, amount: 100 },
  });

  assert.throws(
    () => store.recordEvent("another-command", createEvent(), { fencingToken: 1 }),
    /does not belong to command another-command/
  );
  assert.equal(store.get("another-command").status, "processing");
  assert.equal(store.get("another-command").eventRange, null);
});
