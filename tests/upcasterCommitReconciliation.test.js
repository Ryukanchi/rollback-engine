const test = require("node:test");
const assert = require("node:assert/strict");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { EventUpcasterRegistry } = require("../src/domain/eventUpcaster");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");

const FIXED_TIMESTAMP = "2026-08-20T12:00:00.000Z";

function createOrderUpcasterRegistry() {
  const registry = new EventUpcasterRegistry();
  registry.register({
    eventType: EVENT_TYPES.ORDER_CREATED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        currency: "USD",
      },
    }),
  });
  return registry;
}

function createHarness(storeType, diagnosticReporter = () => {}) {
  const adapters = createStorageAdapters({
    type: storeType,
    upcasterRegistry: createOrderUpcasterRegistry(),
  });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    eventIdGenerator: () => `f6-${storeType}-event`,
    operationIdGenerator: () => `f6-${storeType}-operation`,
    clock: () => FIXED_TIMESTAMP,
    diagnosticReporter,
  });

  return { adapters, engine };
}

function immutableIdentity(event) {
  return {
    eventId: event.eventId,
    aggregateId: event.aggregateId,
    sequence: event.sequence,
    timestamp: event.timestamp,
    eventType: event.eventType,
    commandId: event.metadata.commandId,
    correlationId: event.metadata.correlationId,
    causationId: event.metadata.causationId,
  };
}

for (const storeType of ["memory", "sqlite"]) {
  test(`${storeType}: lost append ACK is reconciled against raw history before upcasting`, (t) => {
    const diagnostics = [];
    const { adapters, engine } = createHarness(storeType, (entry) => {
      diagnostics.push(entry);
    });
    t.after(() => adapters.close());

    const append = adapters.eventStore.append.bind(adapters.eventStore);
    let loseAcknowledgement = true;
    adapters.eventStore.append = (event, options) => {
      const storedEvent = append(event, options);

      if (loseAcknowledgement) {
        loseAcknowledgement = false;
        throw new Error("Append acknowledgement lost");
      }

      return storedEvent;
    };

    const commandId = `f6-${storeType}-lost-ack`;
    const first = engine.createOrder(
      { item: "Book", quantity: 2 },
      { commandId }
    );
    const repeated = engine.createOrder(
      { item: "Book", quantity: 2 },
      { commandId }
    );
    const events = adapters.eventStore.getAll();
    const rawEvents =
      adapters.eventStore.getRawByCommandIdForReconciliation(commandId);

    assert.deepEqual(repeated, first);
    assert.equal(events.length, 1);
    assert.equal(rawEvents.length, 1);
    assert.equal(rawEvents[0].metadata.schemaVersion, 1);
    assert.equal(rawEvents[0].payload.currency, undefined);
    assert.equal(events[0].metadata.schemaVersion, 2);
    assert.equal(events[0].payload.currency, "USD");
    assert.equal(events[0].metadata.commandId, commandId);
    assert.equal(first.state.order.status, "created");
    assert.equal(first.state.order.item, "Book");
    assert.equal(
      diagnostics.some(
        (entry) =>
          entry.type === "EVENT_APPEND" &&
          entry.status === "COMMIT_CONFIRMED_AFTER_ERROR"
      ),
      true
    );
  });

  test(`${storeType}: equal event identity cannot confirm a different raw payload`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());

    const append = adapters.eventStore.append.bind(adapters.eventStore);
    let intendedEvent;
    adapters.eventStore.append = (event, options) => {
      intendedEvent = event;
      const differentRawFact = createDomainEvent({
        ...event,
        payload: {
          ...event.payload,
          item: "Forged Book",
        },
      });
      append(differentRawFact, options);
      throw new Error("Append acknowledgement lost");
    };

    const commandId = `f6-${storeType}-payload-mismatch`;
    assert.throws(
      () =>
        engine.createOrder(
          { item: "Book", quantity: 2 },
          { commandId }
        ),
      (error) =>
        error.code === "COMMAND_EVENT_HISTORY_INCONSISTENT" &&
        error.eventCommitted === true &&
        error.retrySafe === false &&
        error.retryAction === "MANUAL_RESOLUTION_REQUIRED"
    );

    const events = adapters.eventStore.getAll();
    const rawEvents =
      adapters.eventStore.getRawByCommandIdForReconciliation(commandId);
    assert.equal(events.length, 1);
    assert.equal(rawEvents.length, 1);
    assert.deepEqual(immutableIdentity(events[0]), immutableIdentity(intendedEvent));
    assert.deepEqual(immutableIdentity(rawEvents[0]), immutableIdentity(intendedEvent));
    assert.notDeepEqual(rawEvents[0].payload, intendedEvent.payload);
    assert.equal(rawEvents[0].payload.item, "Forged Book");
    assert.equal(rawEvents[0].payload.currency, undefined);
    assert.equal(events[0].payload.item, "Forged Book");
    assert.equal(events[0].payload.currency, "USD");
  });
}
