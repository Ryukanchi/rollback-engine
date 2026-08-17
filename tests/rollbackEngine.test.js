const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const { FAILURE_POINTS } = require("../src/domain/checkoutSaga");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const {
  InMemoryCommandStore,
} = require("../src/infrastructure/inMemoryCommandStore");
const {
  InMemorySnapshotStore,
} = require("../src/infrastructure/inMemorySnapshotStore");
const {
  InMemoryStateRepository,
} = require("../src/infrastructure/inMemoryStateRepository");

function createEngineHarness() {
  const eventStore = new InMemoryEventStore();
  const stateRepository = new InMemoryStateRepository();
  const snapshotStore = new InMemorySnapshotStore();
  let eventId = 0;
  let timestamp = 0;
  const engine = new RollbackEngine({
    eventStore,
    stateRepository,
    snapshotStore,
    eventIdGenerator: () => `event-${++eventId}`,
    clock: () => new Date(Date.UTC(2026, 7, 14, 10, 0, timestamp++)).toISOString(),
  });

  return {
    engine,
    eventStore,
    stateRepository,
    snapshotStore,
  };
}

test("checkout records ordered events and keeps live state equal to replay", () => {
  const { engine } = createEngineHarness();
  const result = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const events = engine.getEvents(result.aggregateId);

  assert.equal(result.status, "completed");
  assert.deepEqual(
    events.map((event) => event.eventType),
    [
      EVENT_TYPES.ORDER_CREATED,
      EVENT_TYPES.INVENTORY_RESERVED,
      EVENT_TYPES.PAYMENT_CHARGED,
    ]
  );
  assert.deepEqual(
    events.map((event) => event.sequence),
    [1, 2, 3]
  );
  assert.deepEqual(result.state, engine.replay(result.aggregateId));
  assert.deepEqual(engine.getLiveState(result.aggregateId), engine.replay(result.aggregateId));
});

test("failed checkout persists compensation events and replayed rollback state", () => {
  const { engine } = createEngineHarness();
  const result = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
    simulateFailureAt: FAILURE_POINTS.AFTER_PAYMENT,
  });

  assert.equal(result.status, "rolled_back");
  assert.deepEqual(
    result.events.map((event) => event.eventType),
    [
      EVENT_TYPES.ORDER_CREATED,
      EVENT_TYPES.INVENTORY_RESERVED,
      EVENT_TYPES.PAYMENT_CHARGED,
      EVENT_TYPES.PAYMENT_REFUNDED,
      EVENT_TYPES.INVENTORY_RELEASED,
      EVENT_TYPES.ORDER_ROLLED_BACK,
    ]
  );
  assert.deepEqual(
    engine.getEvents(result.aggregateId).map((event) => event.sequence),
    [1, 2, 3, 4, 5, 6]
  );
  assert.equal(result.state.payment.status, "refunded");
  assert.equal(result.state.inventory.status, "released");
  assert.equal(result.state.order.status, "rolled_back");
  assert.deepEqual(result.state, engine.replay(result.aggregateId));
});

test("live state equals replay state for every supported failure point", () => {
  for (const simulateFailureAt of Object.values(FAILURE_POINTS)) {
    const { engine } = createEngineHarness();
    const result = engine.checkout({
      item: "Pizza",
      quantity: 1,
      amount: 100,
      simulateFailureAt,
    });

    assert.equal(result.status, "rolled_back");
    assert.deepEqual(
      engine.getLiveState(result.aggregateId),
      engine.replay(result.aggregateId),
      `Live state must equal replay state for ${simulateFailureAt}`
    );
  }
});

test("snapshot plus following compensation events equals full replay", () => {
  const { engine } = createEngineHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const snapshot = engine.createSnapshot(checkout.aggregateId);

  assert.equal(snapshot.version, 3);
  assert.deepEqual(snapshot.state, engine.getLiveState(checkout.aggregateId));

  const compensation = engine.compensate(checkout.aggregateId, "Manual rollback");
  const snapshotReplay = engine.replayFromSnapshot(checkout.aggregateId);
  const fullReplay = engine.replay(checkout.aggregateId);

  assert.equal(compensation.status, "rolled_back");
  assert.equal(compensation.events.length, 3);
  assert.deepEqual(snapshotReplay, fullReplay);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), fullReplay);
});

test("two aggregates maintain independent streams, sequences and states", () => {
  const { engine } = createEngineHarness();
  const first = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const second = engine.checkout({
    item: "Pasta",
    quantity: 2,
    amount: 50,
    simulateFailureAt: FAILURE_POINTS.AFTER_PAYMENT,
  });

  assert.notEqual(first.aggregateId, second.aggregateId);
  assert.deepEqual(
    engine.getEvents(first.aggregateId).map((event) => event.sequence),
    [1, 2, 3]
  );
  assert.deepEqual(
    engine.getEvents(second.aggregateId).map((event) => event.sequence),
    [1, 2, 3, 4, 5, 6]
  );
  assert.equal(engine.getLiveState(first.aggregateId).order.item, "Pizza");
  assert.equal(engine.getLiveState(first.aggregateId).lifecycle, "completed");
  assert.equal(engine.getLiveState(second.aggregateId).order.item, "Pasta");
  assert.equal(engine.getLiveState(second.aggregateId).lifecycle, "rolled_back");
  assert.deepEqual(engine.getLiveState(first.aggregateId), engine.replay(first.aggregateId));
  assert.deepEqual(engine.getLiveState(second.aggregateId), engine.replay(second.aggregateId));
});

test("manual compensation remains idempotent through the engine", () => {
  const { engine } = createEngineHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });

  const first = engine.compensate(checkout.aggregateId, "Manual rollback");
  const eventCount = engine.getEvents(checkout.aggregateId).length;
  const second = engine.compensate(checkout.aggregateId, "Manual rollback");

  assert.equal(first.events.length, 3);
  assert.deepEqual(second.events, []);
  assert.equal(engine.getEvents(checkout.aggregateId).length, eventCount);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), engine.replay(checkout.aggregateId));
});

test("falls back to full replay for corrupted or ahead snapshots", () => {
  const { engine, eventStore } = createEngineHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const fullReplay = engine.replay(checkout.aggregateId);
  const validSnapshot = engine.getSnapshot(checkout.aggregateId);
  const corruptedSnapshot = structuredClone(validSnapshot);

  corruptedSnapshot.state.order.item = "CORRUPTED";

  const corruptedSnapshotEngine = new RollbackEngine({
    eventStore,
    stateRepository: new InMemoryStateRepository(),
    snapshotStore: {
      save: () => {
        throw new Error("Corrupted snapshot store is read-only");
      },
      getByAggregateId: () => structuredClone(corruptedSnapshot),
    },
  });

  assert.deepEqual(
    corruptedSnapshotEngine.replayFromSnapshot(checkout.aggregateId),
    fullReplay
  );

  const aheadSnapshot = structuredClone(validSnapshot);
  aheadSnapshot.version += 1;
  aheadSnapshot.state.version += 1;

  const aheadSnapshotEngine = new RollbackEngine({
    eventStore,
    stateRepository: new InMemoryStateRepository(),
    snapshotStore: {
      save: () => {
        throw new Error("Ahead snapshot store is read-only");
      },
      getByAggregateId: () => structuredClone(aheadSnapshot),
    },
  });

  assert.deepEqual(
    aheadSnapshotEngine.replayFromSnapshot(checkout.aggregateId),
    fullReplay
  );
});

test("repairs the materialized view when its update fails after append", () => {
  class FailOnceStateRepository extends InMemoryStateRepository {
    shouldFail = true;

    save(state) {
      if (this.shouldFail) {
        this.shouldFail = false;
        throw new Error("Materialized view write failed");
      }

      return super.save(state);
    }
  }

  const eventStore = new InMemoryEventStore();
  const stateRepository = new FailOnceStateRepository();
  let eventId = 0;
  let timestamp = 0;
  const engine = new RollbackEngine({
    eventStore,
    stateRepository,
    eventIdGenerator: () => `repair-event-${++eventId}`,
    clock: () => new Date(Date.UTC(2026, 7, 14, 12, 0, timestamp++)).toISOString(),
  });

  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });

  assert.equal(checkout.status, "completed");
  assert.equal(eventStore.getAll().length, 3);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), engine.replay(checkout.aggregateId));
});

test("surfaces an explicit error when the materialized view cannot be repaired", () => {
  class UnavailableStateRepository extends InMemoryStateRepository {
    save() {
      throw new Error("Materialized view unavailable");
    }

    replace() {
      throw new Error("Materialized view unavailable");
    }
  }

  const eventStore = new InMemoryEventStore();
  const engine = new RollbackEngine({
    eventStore,
    stateRepository: new UnavailableStateRepository(),
    eventIdGenerator: () => "committed-event",
    clock: () => "2026-08-14T12:00:00.000Z",
  });

  assert.throws(
    () =>
      engine.checkout({
        item: "Pizza",
        quantity: 1,
        amount: 100,
      }),
    (error) =>
      error.code === "EVENT_COMMITTED_VIEW_REPAIR_FAILED" &&
      error.eventCommitted === true
  );
  assert.deepEqual(
    eventStore.getAll().map((storedEvent) => storedEvent.eventType),
    [EVENT_TYPES.ORDER_CREATED]
  );
});

test("time travel includes all events sharing the same timestamp", () => {
  const engine = new RollbackEngine({
    eventIdGenerator: (() => {
      let eventId = 0;
      return () => `same-time-event-${++eventId}`;
    })(),
    clock: () => "2026-08-14T13:00:00.000Z",
  });
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });

  const stateAtTimestamp = engine.replayAt(
    checkout.aggregateId,
    "2026-08-14T13:00:00.000Z"
  );

  assert.equal(stateAtTimestamp.version, 3);
  assert.deepEqual(stateAtTimestamp, engine.replay(checkout.aggregateId));
});

test("time travel never skips an earlier sequence in a non-monotonic external stream", () => {
  const events = [
    createDomainEvent({
      eventId: "external-event-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 1,
      sequence: 1,
      timestamp: "2026-08-14T14:00:02.000Z",
      payload: { item: "Pizza", quantity: 1 },
    }),
    createDomainEvent({
      eventId: "external-event-2",
      eventType: EVENT_TYPES.INVENTORY_RESERVED,
      aggregateId: 1,
      sequence: 2,
      timestamp: "2026-08-14T14:00:01.000Z",
      payload: { reservationId: 10, item: "Pizza", quantity: 1 },
    }),
  ];
  class NonMonotonicExternalEventStore extends InMemoryEventStore {
    getByAggregateId() {
      return events;
    }
  }
  const engine = new RollbackEngine({
    eventStore: new NonMonotonicExternalEventStore(),
  });

  const stateAtTimestamp = engine.replayAt(1, "2026-08-14T14:00:01.000Z");

  assert.equal(stateAtTimestamp.version, 0);
  assert.equal(stateAtTimestamp.order, null);
});

test("keeps a successful checkout committed when snapshot persistence fails", () => {
  const eventStore = new InMemoryEventStore();
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore,
    snapshotStore: {
      getByAggregateId: () => null,
      save: () => {
        throw new Error("Snapshot store unavailable");
      },
    },
    eventIdGenerator: () => `snapshot-failure-event-${++eventId}`,
    clock: () => "2026-08-14T15:00:00.000Z",
  });

  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });

  assert.equal(checkout.status, "completed");
  assert.equal(checkout.snapshot, null);
  assert.deepEqual(checkout.warnings, [
    {
      code: "SNAPSHOT_SAVE_FAILED",
      category: "technical",
      message: "The command committed successfully, but its snapshot could not be saved.",
      eventCommitted: true,
      retrySafe: false,
      retryAction: "DO_NOT_RETRY_COMMAND",
      aggregateId: checkout.aggregateId,
    },
  ]);
  assert.equal(eventStore.getAll().length, 3);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), engine.replay(checkout.aggregateId));
});

test("keeps a successful delete committed when snapshot persistence fails", () => {
  const eventStore = new InMemoryEventStore();
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore,
    snapshotStore: {
      getByAggregateId: () => null,
      save: () => {
        throw new Error("Snapshot store unavailable");
      },
    },
    eventIdGenerator: () => `delete-snapshot-event-${++eventId}`,
    clock: () => "2026-08-14T15:30:00.000Z",
  });
  const created = engine.createOrder({ item: "Pizza", quantity: 1 });

  const deleted = engine.deleteOrder(created.aggregateId);

  assert.equal(deleted.state.lifecycle, "deleted");
  assert.equal(deleted.snapshot, null);
  assert.deepEqual(deleted.warnings, [
    {
      code: "SNAPSHOT_SAVE_FAILED",
      category: "technical",
      message: "The command committed successfully, but its snapshot could not be saved.",
      eventCommitted: true,
      retrySafe: false,
      retryAction: "DO_NOT_RETRY_COMMAND",
      aggregateId: created.aggregateId,
    },
  ]);
  assert.deepEqual(
    eventStore.getAll().map((event) => event.eventType),
    [EVENT_TYPES.ORDER_CREATED, EVENT_TYPES.ORDER_DELETED]
  );
  assert.deepEqual(engine.getLiveState(created.aggregateId), engine.replay(created.aggregateId));
});

test("repairs a transient materialized-view replace failure after a later event", () => {
  class FailOnceOnVersionTwoRepository extends InMemoryStateRepository {
    shouldFail = true;

    replace(state) {
      if (state.version === 2 && this.shouldFail) {
        this.shouldFail = false;
        throw new Error("Materialized view replace failed");
      }

      return super.replace(state);
    }
  }

  const eventStore = new InMemoryEventStore();
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore,
    stateRepository: new FailOnceOnVersionTwoRepository(),
    eventIdGenerator: () => `late-repair-event-${++eventId}`,
    clock: () => "2026-08-14T16:00:00.000Z",
  });

  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });

  assert.equal(checkout.status, "completed");
  assert.equal(eventStore.getAll().length, 3);
  assert.deepEqual(engine.getLiveState(checkout.aggregateId), engine.replay(checkout.aggregateId));
});

test("marks the second event committed when later view repair remains unavailable", () => {
  class FailOnVersionTwoRepository extends InMemoryStateRepository {
    replace(state) {
      if (state.version === 2) {
        throw new Error("Materialized view replace unavailable");
      }

      return super.replace(state);
    }
  }

  const eventStore = new InMemoryEventStore();
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore,
    stateRepository: new FailOnVersionTwoRepository(),
    eventIdGenerator: () => `late-failure-event-${++eventId}`,
    clock: () => "2026-08-14T16:30:00.000Z",
  });

  assert.throws(
    () =>
      engine.checkout({
        item: "Pizza",
        quantity: 1,
        amount: 100,
      }),
    (error) =>
      error.code === "EVENT_COMMITTED_VIEW_REPAIR_FAILED" &&
      error.eventCommitted === true &&
      error.eventId === "late-failure-event-2"
  );
  assert.deepEqual(
    eventStore.getAll().map((event) => event.eventType),
    [EVENT_TYPES.ORDER_CREATED, EVENT_TYPES.INVENTORY_RESERVED]
  );
  assert.equal(engine.getLiveState(1).version, 1);
  assert.equal(engine.replay(1).version, 2);
});

test("replays the original checkout result for the same idempotency key", () => {
  const { engine, eventStore } = createEngineHarness();
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "checkout-command-1" };

  const first = engine.checkout(command, context);
  const repeated = engine.checkout(command, context);

  assert.deepEqual(repeated, first);
  assert.equal(repeated.aggregateId, first.aggregateId);
  assert.equal(eventStore.getAll().length, 3);
});

test("rejects reuse of an idempotency key with a different command payload", () => {
  const { engine, eventStore } = createEngineHarness();

  engine.checkout(
    { item: "Pizza", quantity: 1, amount: 100 },
    { commandId: "checkout-command-1" }
  );

  assert.throws(
    () =>
      engine.checkout(
        { item: "Pasta", quantity: 1, amount: 100 },
        { commandId: "checkout-command-1" }
      ),
    (error) =>
      error.code === "IDEMPOTENCY_KEY_CONFLICT" &&
      error.commandId === "checkout-command-1" &&
      error.eventCommitted === false &&
      error.retrySafe === false
  );
  assert.equal(eventStore.getAll().length, 3);
});

test("an idempotent delete returns its original result without a second event", () => {
  const { engine, eventStore } = createEngineHarness();
  const created = engine.createOrder(
    { item: "Pizza", quantity: 1 },
    { commandId: "create-order-command-1" }
  );
  const context = { commandId: "delete-order-command-1" };

  const first = engine.deleteOrder(created.aggregateId, "Order deleted", context);
  const repeated = engine.deleteOrder(
    created.aggregateId,
    "Order deleted",
    context
  );

  assert.deepEqual(repeated, first);
  assert.equal(eventStore.getAll().length, 2);
});

test("event metadata forms one correlation and causation chain per command", () => {
  const { engine } = createEngineHarness();

  const result = engine.checkout(
    {
      item: "Pizza",
      quantity: 1,
      amount: 100,
      simulateFailureAt: FAILURE_POINTS.AFTER_PAYMENT,
    },
    {
      commandId: "checkout-command-1",
      correlationId: "checkout-flow-1",
      causationId: "http-request-1",
    }
  );

  assert.equal(result.events.length, 6);
  assert.deepEqual(result.events[0].metadata, {
    schemaVersion: 1,
    commandId: "checkout-command-1",
    correlationId: "checkout-flow-1",
    causationId: "http-request-1",
  });

  for (let index = 1; index < result.events.length; index += 1) {
    assert.deepEqual(result.events[index].metadata, {
      schemaVersion: 1,
      commandId: "checkout-command-1",
      correlationId: "checkout-flow-1",
      causationId: result.events[index - 1].eventId,
    });
  }
});

test("a partially committed idempotent command cannot execute again", () => {
  const eventStore = new InMemoryEventStore();
  let eventId = 0;
  let clockCalls = 0;
  const engine = new RollbackEngine({
    eventStore,
    eventIdGenerator: () => `partial-command-event-${++eventId}`,
    clock: () => {
      clockCalls += 1;

      if (clockCalls === 2) {
        throw new Error("Clock unavailable");
      }

      return "2026-08-15T12:00:00.000Z";
    },
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "partial-checkout-command-1" };

  for (let attempt = 0; attempt < 2; attempt += 1) {
    assert.throws(
      () => engine.checkout(command, context),
      (error) =>
        error.code === "COMMAND_EXECUTION_PARTIALLY_COMMITTED" &&
        error.eventCommitted === true &&
        error.retrySafe === false &&
        error.commandId === context.commandId
    );
  }

  assert.equal(eventStore.getAll().length, 1);
  assert.equal(clockCalls, 2);
});

test("an idempotent domain rejection remains stable after aggregate state changes", () => {
  const { engine, eventStore } = createEngineHarness();
  const checkout = engine.checkout({
    item: "Pizza",
    quantity: 1,
    amount: 100,
  });
  const deleteContext = { commandId: "delete-command-1" };

  assert.throws(
    () =>
      engine.deleteOrder(
        checkout.aggregateId,
        "Order deleted",
        deleteContext
      ),
    (error) =>
      error.code === "COMPENSATION_REQUIRED" &&
      error.eventCommitted === false &&
      error.retrySafe === false
  );

  engine.compensate(checkout.aggregateId, "Manual rollback");

  assert.throws(
    () =>
      engine.deleteOrder(
        checkout.aggregateId,
        "Order deleted",
        deleteContext
      ),
    (error) =>
      error.code === "COMPENSATION_REQUIRED" &&
      error.eventCommitted === false &&
      error.retrySafe === false
  );
  assert.equal(engine.replay(checkout.aggregateId).deleted, false);
  assert.equal(eventStore.getAll().length, 6);
});

test("a keyed technical failure before append releases the command for retry", () => {
  const eventStore = new InMemoryEventStore();
  let eventIdCalls = 0;
  const engine = new RollbackEngine({
    eventStore,
    eventIdGenerator: () => {
      eventIdCalls += 1;

      if (eventIdCalls === 1) {
        throw new Error("Event ID generator unavailable");
      }

      return `retry-event-${eventIdCalls}`;
    },
    clock: () => "2026-08-15T13:00:00.000Z",
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "retryable-checkout-command-1" };

  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.eventCommitted === false &&
      error.retrySafe === true &&
      error.commandId === context.commandId
  );

  const retried = engine.checkout(command, context);

  assert.equal(retried.status, "completed");
  assert.equal(eventStore.getAll().length, 3);
});

test("reconciles an event committed before append acknowledgement was lost", () => {
  class CommitThenThrowEventStore extends InMemoryEventStore {
    shouldLoseAcknowledgement = true;

    append(event, options) {
      const storedEvent = super.append(event, options);

      if (this.shouldLoseAcknowledgement) {
        this.shouldLoseAcknowledgement = false;
        throw new Error("Append acknowledgement lost");
      }

      return storedEvent;
    }
  }

  const eventStore = new CommitThenThrowEventStore();
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore,
    eventIdGenerator: () => `uncertain-append-event-${++eventId}`,
    clock: () => "2026-08-15T14:00:00.000Z",
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "uncertain-checkout-command-1" };

  const first = engine.checkout(command, context);
  const repeated = engine.checkout(command, context);

  assert.deepEqual(repeated, first);
  assert.equal(first.aggregateId, 1);
  assert.equal(eventStore.getAll().length, 3);
  assert.deepEqual(
    eventStore.getAll().map((event) => event.metadata.commandId),
    Array(3).fill(context.commandId)
  );
});

test("does not execute when command events exist without a command record", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const commandId = "missing-command-record";
  const historicalEvent = createDomainEvent({
    eventId: "historical-command-event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: "2026-08-15T14:05:00.000Z",
    payload: { item: "Pizza", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });
  eventStore.append(historicalEvent, { expectedVersion: 0 });
  let generatedEventIds = 0;
  const engine = new RollbackEngine({
    commandStore,
    eventStore,
    eventIdGenerator: () => `unexpected-event-${++generatedEventIds}`,
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };

  for (let attempt = 0; attempt < 2; attempt += 1) {
    assert.throws(
      () => engine.checkout(command, { commandId }),
      (error) =>
        error.code === "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" &&
        error.eventCommitted === true &&
        error.retrySafe === false &&
        error.retryAction === "MANUAL_RESOLUTION_REQUIRED" &&
        error.eventIds?.length === 1 &&
        error.eventIds[0] === historicalEvent.eventId
    );
  }

  assert.equal(generatedEventIds, 0);
  assert.equal(eventStore.getAll().length, 1);
  assert.equal(commandStore.get(commandId).status, "failed");
  assert.deepEqual(commandStore.get(commandId).eventRange.eventIds, [
    historicalEvent.eventId,
  ]);
});

test("rejects one command ID whose events span multiple aggregates", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const commandId = "cross-aggregate-command";

  for (const aggregateId of [1, 2]) {
    eventStore.append(
      createDomainEvent({
        eventId: `cross-aggregate-event-${aggregateId}`,
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId,
        sequence: 1,
        timestamp: "2026-08-15T14:10:00.000Z",
        payload: { item: "Pizza", quantity: 1 },
        metadata: {
          schemaVersion: 1,
          commandId,
          correlationId: commandId,
          causationId: commandId,
        },
      }),
      { expectedVersion: 0 }
    );
  }

  const engine = new RollbackEngine({ commandStore, eventStore });
  const command = { item: "Pizza", quantity: 1, amount: 100 };

  for (let attempt = 0; attempt < 2; attempt += 1) {
    assert.throws(
      () => engine.checkout(command, { commandId }),
      (error) =>
        error.code === "COMMAND_EVENT_HISTORY_INCONSISTENT" &&
        error.eventCommitted === true &&
        error.retrySafe === false &&
        error.retryAction === "MANUAL_RESOLUTION_REQUIRED" &&
        error.eventIds.length === 2
    );
  }

  assert.equal(eventStore.getAll().length, 2);
});

test("keeps an unknown append outcome non-retryable until it can be reconciled", () => {
  class TemporarilyUnreconcilableEventStore extends InMemoryEventStore {
    shouldLoseAcknowledgement = true;

    lookupCalls = 0;

    append(event, options) {
      const storedEvent = super.append(event, options);

      if (this.shouldLoseAcknowledgement) {
        this.shouldLoseAcknowledgement = false;
        throw new Error("Append acknowledgement lost");
      }

      return storedEvent;
    }

    getByCommandId(commandId) {
      this.lookupCalls += 1;

      if (this.lookupCalls === 2) {
        throw new Error("Command event index unavailable");
      }

      return super.getByCommandId(commandId);
    }
  }

  const eventStore = new TemporarilyUnreconcilableEventStore();
  const engine = new RollbackEngine({
    eventStore,
    eventIdGenerator: () => "unknown-append-event-1",
    clock: () => "2026-08-15T14:15:00.000Z",
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "unknown-append-command-1" };

  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.code === "EVENT_APPEND_COMMIT_UNKNOWN" &&
      error.eventCommitted === null &&
      error.retrySafe === false &&
      error.retryAction === "RECONCILE_SAME_KEY"
  );
  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.code === "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" &&
      error.eventCommitted === true &&
      error.retryAction === "MANUAL_RESOLUTION_REQUIRED"
  );
  assert.equal(eventStore.getAll().length, 1);
});

test("retries only after an unknown append is proven uncommitted", () => {
  class UnknownThenUncommittedEventStore extends InMemoryEventStore {
    shouldFailAppend = true;

    lookupCalls = 0;

    append(event, options) {
      if (this.shouldFailAppend) {
        this.shouldFailAppend = false;
        throw new Error("Append unavailable before commit");
      }

      return super.append(event, options);
    }

    getByCommandId(commandId) {
      this.lookupCalls += 1;

      if (this.lookupCalls === 2) {
        throw new Error("Command event index unavailable");
      }

      return super.getByCommandId(commandId);
    }
  }

  const eventStore = new UnknownThenUncommittedEventStore();
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore,
    eventIdGenerator: () => `resolved-uncommitted-event-${++eventId}`,
    clock: () => "2026-08-15T14:20:00.000Z",
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "resolved-uncommitted-command" };

  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.code === "EVENT_APPEND_COMMIT_UNKNOWN" &&
      error.eventCommitted === null &&
      error.retryAction === "RECONCILE_SAME_KEY"
  );

  const result = engine.checkout(command, context);

  assert.equal(result.status, "completed");
  assert.equal(eventStore.getAll().length, 3);
  assert.deepEqual(
    eventStore.getAll().map((event) => event.metadata.commandId),
    Array(3).fill(context.commandId)
  );
});

test("does not execute while a new command reservation cannot be reconciled", () => {
  class LookupFailsOnceEventStore extends InMemoryEventStore {
    lookupCalls = 0;

    getByCommandId(commandId) {
      this.lookupCalls += 1;

      if (this.lookupCalls === 1) {
        throw new Error("Command event index unavailable");
      }

      return super.getByCommandId(commandId);
    }
  }

  const commandStore = new InMemoryCommandStore();
  const eventStore = new LookupFailsOnceEventStore();
  const engine = new RollbackEngine({ commandStore, eventStore });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "preflight-reconciliation-command" };

  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.code === "COMMAND_RECONCILIATION_FAILED" &&
      error.eventCommitted === null &&
      error.retrySafe === false &&
      error.retryAction === "RECONCILE_SAME_KEY"
  );
  assert.equal(eventStore.getAll().length, 0);
  assert.equal(commandStore.get(context.commandId).status, "failed");

  const result = engine.checkout(command, context);

  assert.equal(result.status, "completed");
  assert.equal(eventStore.getAll().length, 3);
});

test("keeps a processing command without events non-executable", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const commandId = "active-processing-command";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { ...command, simulateFailureAt: null },
  });
  const engine = new RollbackEngine({ commandStore, eventStore });

  for (let attempt = 0; attempt < 2; attempt += 1) {
    assert.throws(
      () => engine.checkout(command, { commandId }),
      (error) =>
        error.code === "COMMAND_IN_PROGRESS" &&
        error.eventCommitted === false &&
        error.retrySafe === false &&
        error.retryAction === "WAIT_AND_RETRY_SAME_KEY"
    );
  }

  assert.equal(eventStore.getAll().length, 0);
});

test("rejects a processing event range missing from the Event Store", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const commandId = "processing-command-with-missing-events";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { ...command, simulateFailureAt: null },
  });
  commandStore.recordEvent(
    commandId,
    createDomainEvent({
      eventId: "missing-processing-event-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 1,
      sequence: 1,
      timestamp: "2026-08-15T14:25:00.000Z",
      payload: { item: "Pizza", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    })
  );
  const engine = new RollbackEngine({ commandStore, eventStore });

  assert.throws(
    () => engine.checkout(command, { commandId }),
    (error) =>
      error.code === "COMMAND_EVENT_HISTORY_INCONSISTENT" &&
      error.eventCommitted === false &&
      error.retrySafe === false &&
      error.retryAction === "MANUAL_RESOLUTION_REQUIRED"
  );
  assert.equal(eventStore.getAll().length, 0);
});

test("does not trust a completed command whose events are missing", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const commandId = "orphan-completed-command";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { ...command, simulateFailureAt: null },
  });
  commandStore.complete(commandId, {
    aggregateId: 99,
    status: "completed",
    events: [],
  });
  const engine = new RollbackEngine({ commandStore, eventStore });

  assert.throws(
    () => engine.checkout(command, { commandId }),
    (error) =>
      error.code === "COMMAND_EVENT_HISTORY_INCONSISTENT" &&
      error.eventCommitted === false &&
      error.retrySafe === false &&
      error.retryAction === "MANUAL_RESOLUTION_REQUIRED" &&
      error.eventIds === undefined
  );
  assert.equal(eventStore.getAll().length, 0);
});

test("reconciles a processing command that already has committed events", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const commandId = "interrupted-command-1";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { ...command, simulateFailureAt: null },
  });
  eventStore.append(
    createDomainEvent({
      eventId: "interrupted-event-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 1,
      sequence: 1,
      timestamp: "2026-08-15T14:30:00.000Z",
      payload: { item: "Pizza", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    }),
    { expectedVersion: 0 }
  );
  const engine = new RollbackEngine({ commandStore, eventStore });

  assert.throws(
    () => engine.checkout(command, { commandId }),
    (error) =>
      error.code === "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" &&
      error.eventCommitted === true &&
      error.retrySafe === false &&
      error.retryAction === "MANUAL_RESOLUTION_REQUIRED"
  );
  assert.equal(commandStore.get(commandId).status, "failed");
  assert.equal(commandStore.get(commandId).eventRange.lastSequence, 1);
  assert.equal(eventStore.getAll().length, 1);
});

test("rejects a failed command whose event range omits committed events", () => {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore();
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const commandId = "failed-command-with-missing-range";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { ...command, simulateFailureAt: null },
  });
  commandStore.fail(commandId, {
    code: "COMMAND_EXECUTION_PARTIALLY_COMMITTED",
    message: "Stored failure",
    eventCommitted: true,
    retrySafe: false,
    retryAction: "MANUAL_RESOLUTION_REQUIRED",
  });
  eventStore.append(
    createDomainEvent({
      eventId: "failed-command-event-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 1,
      sequence: 1,
      timestamp: "2026-08-15T14:45:00.000Z",
      payload: { item: "Pizza", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    }),
    { expectedVersion: 0 }
  );
  const engine = new RollbackEngine({ commandStore, eventStore });

  assert.throws(
    () => engine.checkout(command, { commandId }),
    (error) =>
      error.code === "COMMAND_EVENT_HISTORY_INCONSISTENT" &&
      error.eventCommitted === true &&
      error.retrySafe === false &&
      error.retryAction === "MANUAL_RESOLUTION_REQUIRED" &&
      error.eventIds?.length === 1 &&
      error.eventIds[0] === "failed-command-event-1"
  );
  assert.equal(eventStore.getAll().length, 1);
});

test("separate commands can share correlation without sharing command identity", () => {
  const { engine } = createEngineHarness();
  const correlationId = "customer-flow-1";

  const first = engine.createOrder(
    { item: "Pizza", quantity: 1 },
    { commandId: "create-command-1", correlationId }
  );
  const second = engine.createOrder(
    { item: "Pasta", quantity: 1 },
    { commandId: "create-command-2", correlationId }
  );

  assert.equal(first.event.metadata.commandId, "create-command-1");
  assert.equal(second.event.metadata.commandId, "create-command-2");
  assert.equal(first.event.metadata.correlationId, correlationId);
  assert.equal(second.event.metadata.correlationId, correlationId);
  assert.equal(first.event.metadata.causationId, "create-command-1");
  assert.equal(second.event.metadata.causationId, "create-command-2");
  assert.deepEqual(engine.replay(first.aggregateId), first.state);
  assert.deepEqual(engine.replay(second.aggregateId), second.state);
});

test("a complete-store failure becomes a stable committed command failure", () => {
  class FailCompleteOnceCommandStore extends InMemoryCommandStore {
    shouldFail = true;

    complete(commandId, result) {
      if (this.shouldFail) {
        this.shouldFail = false;
        throw new Error("Command result store unavailable");
      }

      return super.complete(commandId, result);
    }
  }

  const eventStore = new InMemoryEventStore();
  const engine = new RollbackEngine({
    eventStore,
    commandStore: new FailCompleteOnceCommandStore(),
    eventIdGenerator: (() => {
      let eventId = 0;
      return () => `complete-store-event-${++eventId}`;
    })(),
    clock: () => "2026-08-15T15:00:00.000Z",
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "complete-store-command-1" };

  for (let attempt = 0; attempt < 2; attempt += 1) {
    assert.throws(
      () => engine.checkout(command, context),
      (error) =>
        error.code === "COMMAND_EXECUTION_PARTIALLY_COMMITTED" &&
        error.retryAction === "MANUAL_RESOLUTION_REQUIRED"
    );
  }
  assert.equal(eventStore.getAll().length, 3);
});

test("a transient fail-store error leaves events protected from re-execution", () => {
  class FailFailureOnceCommandStore extends InMemoryCommandStore {
    shouldFail = true;

    fail(commandId, error) {
      if (this.shouldFail) {
        this.shouldFail = false;
        throw new Error("Command failure store unavailable");
      }

      return super.fail(commandId, error);
    }
  }

  const eventStore = new InMemoryEventStore({ now: () => testTime });
  let clockCalls = 0;
  let testTime = new Date("2026-08-15T15:30:00.000Z").getTime();
  const engine = new RollbackEngine({
    eventStore,
    commandStore: new FailFailureOnceCommandStore({ now: () => testTime }),
    eventIdGenerator: () => "fail-store-event-1",
    clock: () => {
      clockCalls += 1;

      if (clockCalls === 2) {
        throw new Error("Clock unavailable");
      }

      return new Date(testTime).toISOString();
    },
    now: () => testTime,
  });
  const command = { item: "Pizza", quantity: 1, amount: 100 };
  const context = { commandId: "fail-store-command-1" };

  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.code === "COMMAND_STATE_PERSISTENCE_FAILED" &&
      error.eventCommitted === true &&
      error.retryAction === "RECONCILE_SAME_KEY"
  );
  
  // Advance time past the lease TTL so Worker B doesn't see COMMAND_IN_PROGRESS
  testTime += 35000;
  
  assert.throws(
    () => engine.checkout(command, context),
    (error) =>
      error.code === "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" &&
      error.retryAction === "MANUAL_RESOLUTION_REQUIRED"
  );
  assert.equal(eventStore.getAll().length, 1);
});
