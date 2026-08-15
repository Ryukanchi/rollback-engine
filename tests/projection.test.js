const test = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const {
  applyEvent,
  createInitialState,
  projectEvents,
} = require("../src/domain/projection");

const AGGREGATE_ID = 7;
const FAILURE_REASON = "Simulated failure after payment";

const timestamps = [
  "2026-08-14T10:00:00.000Z",
  "2026-08-14T10:00:01.000Z",
  "2026-08-14T10:00:02.000Z",
  "2026-08-14T10:00:03.000Z",
  "2026-08-14T10:00:04.000Z",
  "2026-08-14T10:00:05.000Z",
  "2026-08-14T10:00:06.000Z",
];

function event(eventType, sequence, payload, aggregateId = AGGREGATE_ID) {
  return createDomainEvent({
    eventId: `event-${aggregateId}-${sequence}`,
    eventType,
    aggregateId,
    sequence,
    timestamp: timestamps[sequence - 1],
    payload,
  });
}

function checkoutEvents() {
  return [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 2,
    }),
    event(EVENT_TYPES.INVENTORY_RESERVED, 2, {
      reservationId: 70,
      item: "Pizza",
      quantity: 2,
    }),
    event(EVENT_TYPES.PAYMENT_CHARGED, 3, {
      paymentId: 700,
      amount: 25,
    }),
  ];
}

function rollbackEvents() {
  return [
    ...checkoutEvents(),
    event(EVENT_TYPES.PAYMENT_REFUNDED, 4, {
      paymentId: 700,
      reason: FAILURE_REASON,
    }),
    event(EVENT_TYPES.INVENTORY_RELEASED, 5, {
      reservationId: 70,
      reason: FAILURE_REASON,
    }),
    event(EVENT_TYPES.ORDER_ROLLED_BACK, 6, {
      reason: FAILURE_REASON,
    }),
  ];
}

function deepFreeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);

    for (const nestedValue of Object.values(value)) {
      deepFreeze(nestedValue);
    }
  }

  return value;
}

test("projects a complete successful checkout", () => {
  const state = projectEvents(checkoutEvents());

  assert.deepEqual(state, {
    aggregateId: AGGREGATE_ID,
    version: 3,
    lifecycle: "completed",
    deleted: false,
    tombstone: null,
    order: {
      id: AGGREGATE_ID,
      item: "Pizza",
      quantity: 2,
      status: "created",
      createdAt: timestamps[0],
    },
    inventory: {
      id: 70,
      orderId: AGGREGATE_ID,
      item: "Pizza",
      quantity: 2,
      status: "reserved",
      reservedAt: timestamps[1],
    },
    payment: {
      id: 700,
      orderId: AGGREGATE_ID,
      amount: 25,
      status: "charged",
      chargedAt: timestamps[2],
    },
  });
});

test("projects a complete rollback with compensated entity states", () => {
  const state = projectEvents(rollbackEvents());

  assert.equal(state.version, 6);
  assert.equal(state.lifecycle, "rolled_back");
  assert.equal(state.order.status, "rolled_back");
  assert.equal(state.order.rolledBackAt, timestamps[5]);
  assert.equal(state.order.rollbackReason, FAILURE_REASON);
  assert.equal(state.inventory.status, "released");
  assert.equal(state.inventory.releasedAt, timestamps[4]);
  assert.equal(state.inventory.releaseReason, FAILURE_REASON);
  assert.equal(state.payment.status, "refunded");
  assert.equal(state.payment.refundedAt, timestamps[3]);
  assert.equal(state.payment.refundReason, FAILURE_REASON);
});

test("projects ORDER_DELETED as a tombstone and removes live entities", () => {
  const events = [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 1,
    }),
    event(EVENT_TYPES.ORDER_DELETED, 2, {
      reason: "Customer request",
    }),
  ];

  const state = projectEvents(events);

  assert.deepEqual(state, {
    aggregateId: AGGREGATE_ID,
    version: 2,
    lifecycle: "deleted",
    deleted: true,
    tombstone: {
      aggregateId: AGGREGATE_ID,
      deletedAt: timestamps[1],
      reason: "Customer request",
    },
    order: null,
    inventory: null,
    payment: null,
  });
});

test("applyEvent does not mutate its input state", () => {
  const createdState = projectEvents([checkoutEvents()[0]]);
  const originalState = structuredClone(createdState);
  deepFreeze(createdState);

  const nextState = applyEvent(createdState, checkoutEvents()[1]);

  assert.deepEqual(createdState, originalState);
  assert.notStrictEqual(nextState, createdState);
  assert.equal(nextState.version, 2);
  assert.equal(nextState.inventory.status, "reserved");
});

test("projection rejects unknown events", () => {
  const state = createInitialState(AGGREGATE_ID);
  const unknownEvent = {
    eventId: "event-unknown",
    eventType: "UNKNOWN_EVENT",
    aggregateId: AGGREGATE_ID,
    sequence: 1,
    timestamp: timestamps[0],
    payload: {},
  };

  assert.throws(() => applyEvent(state, unknownEvent), /Unsupported event type: UNKNOWN_EVENT/);
});

test("projection requires contiguous sequences for one aggregate", () => {
  const events = [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 1,
    }),
    event(EVENT_TYPES.INVENTORY_RESERVED, 3, {
      reservationId: 70,
      item: "Pizza",
      quantity: 1,
    }),
  ];

  assert.throws(() => projectEvents(events), /Expected event sequence 2/);
});

test("projection rejects events from another aggregate", () => {
  const state = projectEvents([checkoutEvents()[0]]);
  const foreignEvent = event(
    EVENT_TYPES.INVENTORY_RESERVED,
    2,
    {
      reservationId: 80,
      item: "Pizza",
      quantity: 2,
    },
    8
  );

  assert.throws(() => applyEvent(state, foreignEvent), /does not match state aggregate/);
});

test("projectEvents returns null for an empty event stream", () => {
  assert.equal(projectEvents([]), null);
});

test("projection depends on domain facts but not technical event metadata", () => {
  const canonicalEvents = checkoutEvents();
  const eventsWithInvalidTechnicalMetadata = canonicalEvents.map((domainEvent) => {
    const eventData = structuredClone(domainEvent);
    eventData.metadata = {
      schemaVersion: 999,
      commandId: null,
      correlationId: {},
      causationId: eventData.eventId,
    };
    return eventData;
  });

  assert.deepEqual(
    projectEvents(eventsWithInvalidTechnicalMetadata),
    projectEvents(canonicalEvents)
  );
});

test("projection rejects reactivation after an order was rolled back", () => {
  const events = [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 1,
    }),
    event(EVENT_TYPES.ORDER_ROLLED_BACK, 2, {
      reason: FAILURE_REASON,
    }),
    event(EVENT_TYPES.INVENTORY_RESERVED, 3, {
      reservationId: 70,
      item: "Pizza",
      quantity: 1,
    }),
  ];

  assert.throws(
    () => projectEvents(events),
    /INVENTORY_RESERVED cannot be applied while aggregate 7 is rolled_back/
  );
});

test("projection rejects inventory that does not match the order", () => {
  const wrongItem = [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 2,
    }),
    event(EVENT_TYPES.INVENTORY_RESERVED, 2, {
      reservationId: 70,
      item: "Pasta",
      quantity: 2,
    }),
  ];
  const wrongQuantity = [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 2,
    }),
    event(EVENT_TYPES.INVENTORY_RESERVED, 2, {
      reservationId: 70,
      item: "Pizza",
      quantity: 1,
    }),
  ];

  assert.throws(() => projectEvents(wrongItem), /Reserved item must match order/);
  assert.throws(() => projectEvents(wrongQuantity), /Reserved quantity must match order/);
});

test("projection enforces refund before inventory release", () => {
  const events = [
    ...checkoutEvents(),
    event(EVENT_TYPES.INVENTORY_RELEASED, 4, {
      reservationId: 70,
      reason: FAILURE_REASON,
    }),
  ];

  assert.throws(
    () => projectEvents(events),
    /Payment must be refunded before inventory is released/
  );
});

test("projection rejects deletion while checkout resources remain active", () => {
  const reservedEvents = [
    ...checkoutEvents().slice(0, 2),
    event(EVENT_TYPES.ORDER_DELETED, 3, {
      reason: "Bypass compensation",
    }),
  ];
  const chargedEvents = [
    ...checkoutEvents(),
    event(EVENT_TYPES.ORDER_DELETED, 4, {
      reason: "Bypass compensation",
    }),
  ];

  assert.throws(() => projectEvents(reservedEvents), /must be compensated before it can be deleted/);
  assert.throws(() => projectEvents(chargedEvents), /must be compensated before it can be deleted/);
});

test("projection rejects duplicate rollback facts", () => {
  const events = [
    event(EVENT_TYPES.ORDER_CREATED, 1, {
      item: "Pizza",
      quantity: 1,
    }),
    event(EVENT_TYPES.ORDER_ROLLED_BACK, 2, {
      reason: FAILURE_REASON,
    }),
    event(EVENT_TYPES.ORDER_ROLLED_BACK, 3, {
      reason: FAILURE_REASON,
    }),
  ];

  assert.throws(
    () => projectEvents(events),
    /ORDER_ROLLED_BACK cannot be applied while aggregate 7 is rolled_back/
  );
});
