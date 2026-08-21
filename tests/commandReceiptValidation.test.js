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
    workerId: `receipt-validation-${storeType}`,
    clock: () => "2026-08-21T12:00:00.000Z",
  });

  return { adapters, engine };
}

function interceptCompletedReceiptRead(commandStore, mutateRecord) {
  const originalReserve = commandStore.reserve;
  let manipulatedReads = 0;

  commandStore.reserve = function reserveWithManipulatedReceipt(...args) {
    const reservation = originalReserve.apply(this, args);

    if (
      !reservation.created &&
      !reservation.conflict &&
      reservation.record?.status === "completed"
    ) {
      mutateRecord(reservation.record);
      manipulatedReads += 1;
    }

    return reservation;
  };

  return {
    get manipulatedReads() {
      return manipulatedReads;
    },
    restore() {
      commandStore.reserve = originalReserve;
    },
  };
}

function observeAppendAttempts(eventStore) {
  const originalAppend = eventStore.append;
  let attempts = 0;

  eventStore.append = function observedAppend(...args) {
    attempts += 1;
    return originalAppend.apply(this, args);
  };

  return {
    get attempts() {
      return attempts;
    },
    restore() {
      eventStore.append = originalAppend;
    },
  };
}

function captureOutcome(operation) {
  try {
    return { kind: "returned", value: operation() };
  } catch (error) {
    return { kind: "threw", error };
  }
}

function assertValidEventfulReceipt(record, result, events) {
  assert.equal(record.status, "completed");
  assert.equal(events.length, 1);
  assert.deepEqual(record.eventRange, {
    aggregateId: result.aggregateId,
    firstSequence: 1,
    lastSequence: 1,
    eventIds: [result.event.eventId],
  });
  assert.deepEqual(record.receiptMetadata, {
    contractVersion: 1,
    domainEffect: "events",
    stateAnchor: {
      aggregateId: result.aggregateId,
      sequence: 1,
      lastEventId: result.event.eventId,
    },
  });
  assert.deepEqual(record.result, result);
}

function assertReceiptFailsClosed(outcome, message) {
  assert.equal(
    outcome.kind,
    "threw",
    `${message}; a completed receipt read must fail closed without returning a result`
  );
  assert.equal(outcome.error.code, "COMMAND_RECEIPT_INCONSISTENT");
  assert.equal(outcome.error.retrySafe, false);
  assert.equal(outcome.error.retryAction, "MANUAL_RESOLUTION_REQUIRED");
}

for (const storeType of ["memory", "sqlite"]) {
  test(`manipulated result.state cannot authorize an idempotency result (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const commandId = `receipt-result-state-${storeType}`;
    const command = { item: "Book", quantity: 1 };

    const originalResult = engine.createOrder(command, { commandId });
    const originalRecord = adapters.commandStore.get(commandId);
    const originalEvents = adapters.eventStore.getByAggregateId(
      originalResult.aggregateId
    );

    assertValidEventfulReceipt(originalRecord, originalResult, originalEvents);

    const readInterception = interceptCompletedReceiptRead(
      adapters.commandStore,
      (record) => {
        record.result.state = {
          ...record.result.state,
          lifecycle: "rolled_back",
          order: {
            ...record.result.state.order,
            status: "rolled_back",
          },
        };
      }
    );
    const appendObservation = observeAppendAttempts(adapters.eventStore);
    t.after(() => readInterception.restore());
    t.after(() => appendObservation.restore());

    const outcome = captureOutcome(() =>
      engine.createOrder(command, { commandId })
    );

    assert.equal(readInterception.manipulatedReads, 1);
    assert.equal(appendObservation.attempts, 0, "the command must not re-execute");
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(originalResult.aggregateId),
      originalEvents,
      "receipt rejection must not change domain history"
    );
    assert.deepEqual(
      adapters.commandStore.get(commandId),
      originalRecord,
      "the read-path manipulation must not alter the persisted receipt"
    );

    if (outcome.kind === "returned") {
      assert.equal(outcome.value.state.lifecycle, "rolled_back");
      assert.equal(outcome.value.state.order.status, "rolled_back");
    }

    assertReceiptFailsClosed(
      outcome,
      "F-12 currently returns the manipulated derived state"
    );
  });

  test(`a wrong stateAnchor.lastEventId invalidates the completed receipt (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const commandId = `receipt-state-anchor-${storeType}`;
    const command = { item: "Book", quantity: 1 };

    const originalResult = engine.createOrder(command, { commandId });
    const originalRecord = adapters.commandStore.get(commandId);
    const originalEvents = adapters.eventStore.getByAggregateId(
      originalResult.aggregateId
    );

    assertValidEventfulReceipt(originalRecord, originalResult, originalEvents);

    const readInterception = interceptCompletedReceiptRead(
      adapters.commandStore,
      (record) => {
        record.receiptMetadata.stateAnchor.lastEventId =
          `not-the-committed-event-${storeType}`;
      }
    );
    const appendObservation = observeAppendAttempts(adapters.eventStore);
    t.after(() => readInterception.restore());
    t.after(() => appendObservation.restore());

    const outcome = captureOutcome(() =>
      engine.createOrder(command, { commandId })
    );

    assert.equal(readInterception.manipulatedReads, 1);
    assert.equal(appendObservation.attempts, 0, "the command must not re-execute");
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(originalResult.aggregateId),
      originalEvents,
      "receipt rejection must not change domain history"
    );
    assert.deepEqual(
      adapters.commandStore.get(commandId),
      originalRecord,
      "the read-path manipulation must not alter the persisted receipt"
    );

    if (outcome.kind === "returned") {
      assert.deepEqual(outcome.value, originalResult);
    }

    assertReceiptFailsClosed(
      outcome,
      "F-12 currently ignores the mismatching historical state anchor"
    );
  });

  test(`a completed receipt without its contract envelope fails closed (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const commandId = `receipt-missing-envelope-${storeType}`;
    const command = { item: "Book", quantity: 1 };

    const originalResult = engine.createOrder(command, { commandId });
    const originalRecord = adapters.commandStore.get(commandId);
    const originalEvents = adapters.eventStore.getByAggregateId(
      originalResult.aggregateId
    );

    assertValidEventfulReceipt(originalRecord, originalResult, originalEvents);

    const readInterception = interceptCompletedReceiptRead(
      adapters.commandStore,
      (record) => {
        record.receiptMetadata = null;
      }
    );
    const appendObservation = observeAppendAttempts(adapters.eventStore);
    t.after(() => readInterception.restore());
    t.after(() => appendObservation.restore());

    const outcome = captureOutcome(() =>
      engine.createOrder(command, { commandId })
    );

    assert.equal(readInterception.manipulatedReads, 1);
    assert.equal(appendObservation.attempts, 0, "the command must not re-execute");
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(originalResult.aggregateId),
      originalEvents
    );
    assert.deepEqual(adapters.commandStore.get(commandId), originalRecord);
    assertReceiptFailsClosed(
      outcome,
      "an unanchored legacy receipt cannot prove a safe idempotency result"
    );
  });

  test(`receipt validation preserves a historical result after the aggregate head advances (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const command = { item: "Book", quantity: 1 };
    const commandId = `receipt-historical-head-${storeType}`;

    const originalResult = engine.createOrder(command, { commandId });
    engine.deleteOrder(
      originalResult.aggregateId,
      "Advance aggregate history",
      { commandId: `receipt-historical-head-delete-${storeType}` }
    );
    const eventsBeforeRetry = adapters.eventStore.getByAggregateId(
      originalResult.aggregateId
    );

    const repeatedResult = engine.createOrder(command, { commandId });

    assert.deepEqual(repeatedResult, originalResult);
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(originalResult.aggregateId),
      eventsBeforeRetry
    );
    assert.equal(eventsBeforeRetry.length, 2);
    assert.equal(originalResult.state.version, 1);
    assert.equal(eventsBeforeRetry.at(-1).sequence, 2);
  });

  test(`receipt replay ignores a corrupted materialized view (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const command = { item: "Book", quantity: 1 };
    const commandId = `receipt-corrupt-view-${storeType}`;

    const originalResult = engine.createOrder(command, { commandId });
    const originalEvents = adapters.eventStore.getByAggregateId(
      originalResult.aggregateId
    );
    const corruptedView = {
      ...originalResult.state,
      lifecycle: "rolled_back",
      order: {
        ...originalResult.state.order,
        status: "rolled_back",
      },
    };
    adapters.stateRepository.replace(corruptedView);

    const repeatedResult = engine.createOrder(command, { commandId });

    assert.deepEqual(repeatedResult, originalResult);
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(originalResult.aggregateId),
      originalEvents
    );
    assert.deepEqual(
      adapters.stateRepository.getByAggregateId(originalResult.aggregateId),
      corruptedView,
      "receipt validation must neither trust nor repair the materialized view"
    );
  });

  test(`a manipulated result.snapshot cannot authorize an idempotency result (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const command = { item: "Book", quantity: 1, amount: 20 };
    const commandId = `receipt-result-snapshot-${storeType}`;

    const originalResult = engine.checkout(command, { commandId });
    const originalRecord = adapters.commandStore.get(commandId);
    const originalEvents = adapters.eventStore.getByAggregateId(
      originalResult.aggregateId
    );
    assert.ok(originalResult.snapshot);

    const readInterception = interceptCompletedReceiptRead(
      adapters.commandStore,
      (record) => {
        record.result.snapshot.state = {
          ...record.result.snapshot.state,
          lifecycle: "rolled_back",
        };
      }
    );
    const appendObservation = observeAppendAttempts(adapters.eventStore);
    t.after(() => readInterception.restore());
    t.after(() => appendObservation.restore());

    const outcome = captureOutcome(() =>
      engine.checkout(command, { commandId })
    );

    assert.equal(readInterception.manipulatedReads, 1);
    assert.equal(appendObservation.attempts, 0, "the command must not re-execute");
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(originalResult.aggregateId),
      originalEvents
    );
    assert.deepEqual(adapters.commandStore.get(commandId), originalRecord);

    if (outcome.kind === "returned") {
      assert.equal(outcome.value.snapshot.state.lifecycle, "rolled_back");
    }

    assertReceiptFailsClosed(
      outcome,
      "F-12 currently returns a snapshot that disagrees with historical replay"
    );
  });

  test(`an eventless completed receipt retains its historical anchor (${storeType})`, (t) => {
    const { adapters, engine } = createHarness(storeType);
    t.after(() => adapters.close());
    const setup = engine.checkout(
      {
        item: "Book",
        quantity: 1,
        amount: 20,
        simulateFailureAt: "after_payment",
      },
      { commandId: `receipt-eventless-setup-${storeType}` }
    );
    const commandId = `receipt-eventless-${storeType}`;
    const options = { commandId };

    const originalResult = engine.compensate(
      setup.aggregateId,
      "Already compensated",
      options
    );
    const eventsBeforeRetry = adapters.eventStore.getByAggregateId(
      setup.aggregateId
    );
    const repeatedResult = engine.compensate(
      setup.aggregateId,
      "Already compensated",
      options
    );

    assert.equal(originalResult.events.length, 0);
    assert.deepEqual(repeatedResult, originalResult);
    assert.deepEqual(
      adapters.eventStore.getByAggregateId(setup.aggregateId),
      eventsBeforeRetry
    );
    assert.deepEqual(adapters.commandStore.get(commandId).receiptMetadata, {
      contractVersion: 1,
      domainEffect: "none",
      stateAnchor: {
        aggregateId: setup.aggregateId,
        sequence: originalResult.state.version,
        lastEventId: eventsBeforeRetry.at(-1).eventId,
      },
    });
  });
}
