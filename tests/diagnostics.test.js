const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const {
  InMemorySnapshotStore,
} = require("../src/infrastructure/inMemorySnapshotStore");
const {
  InMemoryStateRepository,
} = require("../src/infrastructure/inMemoryStateRepository");

function checkoutCommand() {
  return { item: "Pizza", quantity: 1, amount: 100 };
}

test("reports an append that committed before its acknowledgement was lost", () => {
  class CommitThenThrowEventStore extends InMemoryEventStore {
    loseAcknowledgement = true;

    append(event, options) {
      const storedEvent = super.append(event, options);

      if (this.loseAcknowledgement) {
        this.loseAcknowledgement = false;
        throw new Error("Append acknowledgement lost");
      }

      return storedEvent;
    }
  }

  const diagnostics = [];
  let eventId = 0;
  const engine = new RollbackEngine({
    eventStore: new CommitThenThrowEventStore(),
    eventIdGenerator: () => `diagnostic-event-${++eventId}`,
    clock: () => "2026-08-15T10:00:00.000Z",
    diagnosticReporter: (diagnostic) => diagnostics.push(diagnostic),
  });

  const result = engine.checkout(checkoutCommand(), {
    commandId: "diagnostic-command",
  });

  assert.equal(result.status, "completed");
  assert.equal(diagnostics.length, 1);
  assert.deepEqual(
    {
      ...diagnostics[0],
      occurredAt: "<normalized>",
    },
    {
      type: "EVENT_APPEND",
      status: "COMMIT_CONFIRMED_AFTER_ERROR",
      occurredAt: "<normalized>",
      commandId: "diagnostic-command",
      aggregateId: 1,
      eventId: "diagnostic-event-1",
    }
  );
  assert.equal(new Date(diagnostics[0].occurredAt).toISOString(), diagnostics[0].occurredAt);
  assert.equal(Object.isFrozen(diagnostics[0]), true);
});

test("reports an unavailable command index as a reconciliation failure", () => {
  class UnavailableCommandIndexEventStore extends InMemoryEventStore {
    getByCommandId() {
      throw new Error("Command index unavailable");
    }
  }

  const diagnostics = [];
  const engine = new RollbackEngine({
    eventStore: new UnavailableCommandIndexEventStore(),
    diagnosticReporter: (diagnostic) => diagnostics.push(diagnostic),
  });

  assert.throws(
    () =>
      engine.checkout(checkoutCommand(), {
        commandId: "reconciliation-command",
      }),
    (error) => error.code === "COMMAND_RECONCILIATION_FAILED"
  );
  assert.deepEqual(
    diagnostics.map(({ occurredAt, ...diagnostic }) => diagnostic),
    [
      {
        type: "COMMAND_RECONCILIATION",
        status: "LOOKUP_FAILED",
        commandId: "reconciliation-command",
      },
    ]
  );
});

test("reports a successful materialized-view repair after an initial write failure", () => {
  class FailOnceStateRepository extends InMemoryStateRepository {
    failNextSave = true;

    save(state) {
      if (this.failNextSave) {
        this.failNextSave = false;
        throw new Error("State write failed once");
      }

      return super.save(state);
    }
  }

  const diagnostics = [];
  const engine = new RollbackEngine({
    stateRepository: new FailOnceStateRepository(),
    eventIdGenerator: () => "repair-diagnostic-event",
    operationIdGenerator: () => "repair-command",
    clock: () => "2026-08-15T10:00:00.000Z",
    diagnosticReporter: (diagnostic) => diagnostics.push(diagnostic),
  });

  const result = engine.createOrder({ item: "Pizza", quantity: 1 });

  assert.equal(result.state.order.status, "created");
  assert.deepEqual(
    diagnostics.map(({ occurredAt, ...diagnostic }) => diagnostic),
    [
      {
        type: "MATERIALIZED_VIEW_REPAIR",
        status: "REPAIRED",
        commandId: "repair-command",
        aggregateId: 1,
        eventId: "repair-diagnostic-event",
      },
    ]
  );
});

test("reports a failed materialized-view repair after the event was committed", () => {
  class UnavailableStateRepository extends InMemoryStateRepository {
    save() {
      throw new Error("State repository unavailable");
    }
  }

  const diagnostics = [];
  const eventStore = new InMemoryEventStore();
  const engine = new RollbackEngine({
    eventStore,
    stateRepository: new UnavailableStateRepository(),
    eventIdGenerator: () => "failed-repair-event",
    operationIdGenerator: () => "failed-repair-command",
    clock: () => "2026-08-15T10:00:00.000Z",
    diagnosticReporter: (diagnostic) => diagnostics.push(diagnostic),
  });

  assert.throws(
    () => engine.createOrder({ item: "Pizza", quantity: 1 }),
    (error) =>
      error.code === "EVENT_COMMITTED_VIEW_REPAIR_FAILED" &&
      error.eventCommitted === true
  );
  assert.equal(eventStore.getByAggregateId(1).length, 1);
  assert.deepEqual(
    diagnostics.map(({ occurredAt, ...diagnostic }) => diagnostic),
    [
      {
        type: "MATERIALIZED_VIEW_REPAIR",
        status: "REPAIR_FAILED",
        commandId: "failed-repair-command",
        aggregateId: 1,
        eventId: "failed-repair-event",
      },
    ]
  );
});

test("reports snapshot persistence failure outside the domain commit boundary", () => {
  class UnavailableSnapshotStore extends InMemorySnapshotStore {
    save() {
      throw new Error("Snapshot store unavailable");
    }
  }

  const diagnostics = [];
  const engine = new RollbackEngine({
    snapshotStore: new UnavailableSnapshotStore(),
    diagnosticReporter: (diagnostic) => diagnostics.push(diagnostic),
  });

  const result = engine.checkout(checkoutCommand(), {
    commandId: "snapshot-diagnostic-command",
  });

  assert.equal(result.status, "completed");
  assert.equal(result.warnings[0].code, "SNAPSHOT_SAVE_FAILED");
  assert.deepEqual(
    diagnostics.map(({ occurredAt, ...diagnostic }) => diagnostic),
    [
      {
        type: "SNAPSHOT_SAVE",
        status: "SAVE_FAILED",
        commandId: "snapshot-diagnostic-command",
        aggregateId: result.aggregateId,
      },
    ]
  );
});

test("diagnostic reporter failures cannot change committed command semantics", () => {
  class UnavailableSnapshotStore extends InMemorySnapshotStore {
    save() {
      throw new Error("Snapshot store unavailable");
    }
  }

  let reporterCalls = 0;
  const engine = new RollbackEngine({
    snapshotStore: new UnavailableSnapshotStore(),
    diagnosticReporter: () => {
      reporterCalls += 1;
      throw new Error("Diagnostic backend unavailable");
    },
  });

  const result = engine.checkout(checkoutCommand());

  assert.equal(result.status, "completed");
  assert.equal(result.warnings[0].code, "SNAPSHOT_SAVE_FAILED");
  assert.equal(reporterCalls, 1);
  assert.equal(engine.getEvents(result.aggregateId).length, 3);
});
