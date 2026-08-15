const test = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const {
  FAILURE_POINTS,
  compensateCheckout,
  runCheckoutSaga,
} = require("../src/domain/checkoutSaga");
const { applyEvent, createInitialState } = require("../src/domain/projection");

function createSagaHarness(aggregateId = 1) {
  const events = [];
  let state = null;

  return {
    events,
    dependencies: {
      recordEvent(eventType, payload) {
        const sequence = events.length + 1;
        const domainEvent = createDomainEvent({
          eventId: `event-${aggregateId}-${sequence}`,
          eventType,
          aggregateId,
          sequence,
          timestamp: `2026-08-14T10:00:0${sequence}.000Z`,
          payload,
        });

        state = applyEvent(state || createInitialState(aggregateId), domainEvent);
        events.push(domainEvent);
        return domainEvent;
      },
      getState() {
        return state ? structuredClone(state) : null;
      },
    },
  };
}

function context(overrides = {}) {
  return {
    aggregateId: 1,
    reservationId: 10,
    paymentId: 100,
    item: "Pizza",
    quantity: 1,
    amount: 25,
    ...overrides,
  };
}

test("runs a successful checkout in forward order", () => {
  const harness = createSagaHarness();
  const result = runCheckoutSaga(context(), harness.dependencies);

  assert.equal(result.status, "completed");
  assert.equal(result.error, null);
  assert.deepEqual(
    result.events.map((event) => event.eventType),
    [
      EVENT_TYPES.ORDER_CREATED,
      EVENT_TYPES.INVENTORY_RESERVED,
      EVENT_TYPES.PAYMENT_CHARGED,
    ]
  );
  assert.equal(harness.dependencies.getState().lifecycle, "completed");
});

test("compensates a failure after payment in reverse order", () => {
  const harness = createSagaHarness();
  const result = runCheckoutSaga(
    context({ simulateFailureAt: FAILURE_POINTS.AFTER_PAYMENT }),
    harness.dependencies
  );

  assert.equal(result.status, "rolled_back");
  assert.equal(result.failedAt, FAILURE_POINTS.AFTER_PAYMENT);
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
    result.compensationEvents.map((event) => event.eventType),
    [
      EVENT_TYPES.PAYMENT_REFUNDED,
      EVENT_TYPES.INVENTORY_RELEASED,
      EVENT_TYPES.ORDER_ROLLED_BACK,
    ]
  );
});

test("compensates only completed steps after a partial failure", () => {
  const harness = createSagaHarness();
  const result = runCheckoutSaga(
    context({ simulateFailureAt: FAILURE_POINTS.AFTER_INVENTORY }),
    harness.dependencies
  );

  assert.deepEqual(
    result.events.map((event) => event.eventType),
    [
      EVENT_TYPES.ORDER_CREATED,
      EVENT_TYPES.INVENTORY_RESERVED,
      EVENT_TYPES.INVENTORY_RELEASED,
      EVENT_TYPES.ORDER_ROLLED_BACK,
    ]
  );
  assert.equal(harness.dependencies.getState().payment, null);
  assert.equal(harness.dependencies.getState().inventory.status, "released");
  assert.equal(harness.dependencies.getState().order.status, "rolled_back");
});

test("compensation is idempotent for an already rolled-back checkout", () => {
  const harness = createSagaHarness();

  runCheckoutSaga(context(), harness.dependencies);

  const firstCompensation = compensateCheckout(
    { reason: "Manual rollback" },
    harness.dependencies
  );
  const eventCountAfterFirstCompensation = harness.events.length;
  const secondCompensation = compensateCheckout(
    { reason: "Manual rollback" },
    harness.dependencies
  );

  assert.deepEqual(
    firstCompensation.map((event) => event.eventType),
    [
      EVENT_TYPES.PAYMENT_REFUNDED,
      EVENT_TYPES.INVENTORY_RELEASED,
      EVENT_TYPES.ORDER_ROLLED_BACK,
    ]
  );
  assert.deepEqual(secondCompensation, []);
  assert.equal(harness.events.length, eventCountAfterFirstCompensation);
  assert.equal(harness.dependencies.getState().lifecycle, "rolled_back");
});

test("does not compensate infrastructure or programming failures", () => {
  const harness = createSagaHarness();
  const recordEvent = harness.dependencies.recordEvent;
  let callCount = 0;
  const dependencies = {
    ...harness.dependencies,
    recordEvent(eventType, payload) {
      callCount += 1;

      if (callCount === 2) {
        const error = new Error("Event store unavailable");
        error.code = "EVENT_STORE_UNAVAILABLE";
        throw error;
      }

      return recordEvent(eventType, payload);
    },
  };

  assert.throws(
    () => runCheckoutSaga(context(), dependencies),
    (error) => error.code === "EVENT_STORE_UNAVAILABLE"
  );
  assert.deepEqual(
    harness.events.map((storedEvent) => storedEvent.eventType),
    [EVENT_TYPES.ORDER_CREATED]
  );
});
