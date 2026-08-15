const { EVENT_TYPES, assertDomainFact } = require("./events");

const LIFECYCLES = new Set([
  "empty",
  "active",
  "completed",
  "compensating",
  "rolled_back",
  "deleted",
]);

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function createInitialState(aggregateId) {
  if (!isIdentifier(aggregateId)) {
    throw new TypeError("aggregateId must be a non-empty string or a positive safe integer");
  }

  return {
    aggregateId,
    version: 0,
    lifecycle: "empty",
    deleted: false,
    tombstone: null,
    order: null,
    inventory: null,
    payment: null,
  };
}

function assertProjectionState(state) {
  if (!state || typeof state !== "object" || Array.isArray(state)) {
    throw new TypeError("state must be an object");
  }

  if (!isIdentifier(state.aggregateId)) {
    throw new TypeError("state.aggregateId must be a valid identifier");
  }

  if (!Number.isSafeInteger(state.version) || state.version < 0) {
    throw new TypeError("state.version must be a non-negative safe integer");
  }

  if (!LIFECYCLES.has(state.lifecycle)) {
    throw new TypeError(`Unsupported state lifecycle: ${state.lifecycle}`);
  }

  if (typeof state.deleted !== "boolean") {
    throw new TypeError("state.deleted must be a boolean");
  }
}

function assertEventCanBeApplied(state, event) {
  if (event.aggregateId !== state.aggregateId) {
    throw new Error(
      `Event aggregate ${event.aggregateId} does not match state aggregate ${state.aggregateId}`
    );
  }

  const expectedSequence = state.version + 1;

  if (event.sequence !== expectedSequence) {
    throw new Error(
      `Expected event sequence ${expectedSequence} for aggregate ${state.aggregateId}, received ${event.sequence}`
    );
  }
}

function assertNotDeleted(state, eventType) {
  if (state.deleted) {
    throw new Error(`${eventType} cannot be applied to deleted aggregate ${state.aggregateId}`);
  }
}

function assertLifecycle(state, eventType, allowedLifecycles) {
  if (!allowedLifecycles.includes(state.lifecycle)) {
    throw new Error(
      `${eventType} cannot be applied while aggregate ${state.aggregateId} is ${state.lifecycle}`
    );
  }
}

function applyEvent(state, event) {
  assertProjectionState(state);
  assertDomainFact(event);
  assertEventCanBeApplied(state, event);

  switch (event.eventType) {
    case EVENT_TYPES.ORDER_CREATED: {
      assertLifecycle(state, event.eventType, ["empty"]);

      if (state.version !== 0 || state.order) {
        throw new Error(`Order ${state.aggregateId} has already been created`);
      }

      return {
        ...state,
        version: event.sequence,
        lifecycle: "active",
        deleted: false,
        tombstone: null,
        order: {
          id: event.aggregateId,
          item: event.payload.item,
          quantity: event.payload.quantity,
          status: "created",
          createdAt: event.timestamp,
        },
      };
    }

    case EVENT_TYPES.INVENTORY_RESERVED: {
      assertNotDeleted(state, event.eventType);
      assertLifecycle(state, event.eventType, ["active"]);

      if (!state.order || state.order.status !== "created") {
        throw new Error(`Inventory cannot be reserved before order ${state.aggregateId} exists`);
      }

      if (state.inventory) {
        throw new Error(`Inventory has already been reserved for order ${state.aggregateId}`);
      }

      if (state.payment) {
        throw new Error(`Inventory cannot be reserved after payment for order ${state.aggregateId}`);
      }

      if (event.payload.item !== state.order.item) {
        throw new Error(`Reserved item must match order ${state.aggregateId}`);
      }

      if (event.payload.quantity !== state.order.quantity) {
        throw new Error(`Reserved quantity must match order ${state.aggregateId}`);
      }

      return {
        ...state,
        version: event.sequence,
        inventory: {
          id: event.payload.reservationId,
          orderId: event.aggregateId,
          item: event.payload.item,
          quantity: event.payload.quantity,
          status: "reserved",
          reservedAt: event.timestamp,
        },
      };
    }

    case EVENT_TYPES.PAYMENT_CHARGED: {
      assertNotDeleted(state, event.eventType);
      assertLifecycle(state, event.eventType, ["active"]);

      if (!state.order || state.order.status !== "created") {
        throw new Error(`Order ${state.aggregateId} must be active before payment can be charged`);
      }

      if (!state.inventory || state.inventory.status !== "reserved") {
        throw new Error(`Payment cannot be charged before inventory is reserved for order ${state.aggregateId}`);
      }

      if (state.payment) {
        throw new Error(`Payment has already been recorded for order ${state.aggregateId}`);
      }

      return {
        ...state,
        version: event.sequence,
        lifecycle: "completed",
        payment: {
          id: event.payload.paymentId,
          orderId: event.aggregateId,
          amount: event.payload.amount,
          status: "charged",
          chargedAt: event.timestamp,
        },
      };
    }

    case EVENT_TYPES.PAYMENT_REFUNDED: {
      assertNotDeleted(state, event.eventType);
      assertLifecycle(state, event.eventType, ["completed"]);

      if (!state.payment || state.payment.status !== "charged") {
        throw new Error(`A charged payment is required to refund order ${state.aggregateId}`);
      }

      if (!state.inventory || state.inventory.status !== "reserved") {
        throw new Error(`Reserved inventory is required before refunding order ${state.aggregateId}`);
      }

      if (state.payment.id !== event.payload.paymentId) {
        throw new Error(`Payment ID does not match order ${state.aggregateId}`);
      }

      return {
        ...state,
        version: event.sequence,
        lifecycle: "compensating",
        payment: {
          ...state.payment,
          status: "refunded",
          refundedAt: event.timestamp,
          refundReason: event.payload.reason || null,
        },
      };
    }

    case EVENT_TYPES.INVENTORY_RELEASED: {
      assertNotDeleted(state, event.eventType);

      if (!state.inventory || state.inventory.status !== "reserved") {
        throw new Error(`Reserved inventory is required to release order ${state.aggregateId}`);
      }

      if (state.inventory.id !== event.payload.reservationId) {
        throw new Error(`Reservation ID does not match order ${state.aggregateId}`);
      }

      if (state.payment && state.payment.status !== "refunded") {
        throw new Error(`Payment must be refunded before inventory is released for order ${state.aggregateId}`);
      }

      assertLifecycle(state, event.eventType, ["active", "compensating"]);

      return {
        ...state,
        version: event.sequence,
        lifecycle: "compensating",
        inventory: {
          ...state.inventory,
          status: "released",
          releasedAt: event.timestamp,
          releaseReason: event.payload.reason || null,
        },
      };
    }

    case EVENT_TYPES.ORDER_ROLLED_BACK: {
      assertNotDeleted(state, event.eventType);
      assertLifecycle(state, event.eventType, ["active", "compensating"]);

      if (!state.order || state.order.status !== "created") {
        throw new Error(`Order ${state.aggregateId} must exist before it can be rolled back`);
      }

      if (state.payment && state.payment.status !== "refunded") {
        throw new Error(`Payment must be refunded before order ${state.aggregateId} is rolled back`);
      }

      if (state.inventory && state.inventory.status !== "released") {
        throw new Error(`Inventory must be released before order ${state.aggregateId} is rolled back`);
      }

      return {
        ...state,
        version: event.sequence,
        lifecycle: "rolled_back",
        order: {
          ...state.order,
          status: "rolled_back",
          rolledBackAt: event.timestamp,
          rollbackReason: event.payload.reason || null,
        },
      };
    }

    case EVENT_TYPES.ORDER_DELETED: {
      assertNotDeleted(state, event.eventType);

      if (!state.order) {
        throw new Error(`Order ${state.aggregateId} must exist before it can be deleted`);
      }

      if (
        state.payment?.status === "charged" ||
        state.inventory?.status === "reserved"
      ) {
        throw new Error(`Order ${state.aggregateId} must be compensated before it can be deleted`);
      }

      assertLifecycle(state, event.eventType, ["active", "rolled_back"]);

      return {
        ...state,
        version: event.sequence,
        lifecycle: "deleted",
        deleted: true,
        tombstone: {
          aggregateId: event.aggregateId,
          deletedAt: event.timestamp,
          reason: event.payload.reason || null,
        },
        order: null,
        inventory: null,
        payment: null,
      };
    }

    default:
      throw new TypeError(`Unsupported event type: ${event.eventType}`);
  }
}

function projectEvents(events) {
  if (!Array.isArray(events)) {
    throw new TypeError("events must be an array");
  }

  if (events.length === 0) {
    return null;
  }

  let state = createInitialState(events[0].aggregateId);

  for (const event of events) {
    state = applyEvent(state, event);
  }

  return state;
}

module.exports = {
  applyEvent,
  createInitialState,
  projectEvents,
};
