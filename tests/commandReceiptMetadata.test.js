const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");

function createHarness(storeType) {
  const adapters = createStorageAdapters({ type: storeType });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    workerId: `receipt-${storeType}`,
    clock: () => "2026-08-21T10:00:00.000Z",
  });

  return { adapters, engine };
}

for (const storeType of ["memory", "sqlite"]) {
  test(`completed eventful commands persist a historical state anchor (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const commandId = `receipt-events-${storeType}`;

    const result = engine.createOrder(
      { item: "Book", quantity: 1 },
      { commandId }
    );
    const command = adapters.commandStore.get(commandId);

    assert.equal(command.status, "completed");
    assert.deepEqual(command.receiptMetadata, {
      contractVersion: 1,
      domainEffect: "events",
      stateAnchor: {
        aggregateId: result.aggregateId,
        sequence: result.state.version,
        lastEventId: result.event.eventId,
      },
    });
  });

  test(`successful eventless commands retain a state anchor without claiming an effect range (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const setup = engine.checkout(
      {
        item: "Book",
        quantity: 1,
        amount: 20,
        simulateFailureAt: "after_payment",
      },
      { commandId: `receipt-none-setup-${storeType}` }
    );
    const commandId = `receipt-none-${storeType}`;

    const result = engine.compensate(
      setup.aggregateId,
      "already compensated",
      { commandId }
    );
    const aggregateEvents = adapters.eventStore.getByAggregateId(setup.aggregateId);
    const command = adapters.commandStore.get(commandId);

    assert.equal(result.events.length, 0);
    assert.equal(command.eventRange, null);
    assert.deepEqual(command.receiptMetadata, {
      contractVersion: 1,
      domainEffect: "none",
      stateAnchor: {
        aggregateId: setup.aggregateId,
        sequence: result.state.version,
        lastEventId: aggregateEvents.at(-1).eventId,
      },
    });
  });
}
