const { EVENT_TYPES } = require("./events");

const FAILURE_POINTS = Object.freeze({
  AFTER_ORDER: "after_order",
  AFTER_INVENTORY: "after_inventory",
  AFTER_PAYMENT: "after_payment",
});

const SUPPORTED_FAILURE_POINTS = new Set(Object.values(FAILURE_POINTS));

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function validateCheckoutCommand(command) {
  if (!command || typeof command !== "object" || Array.isArray(command)) {
    throw new TypeError("checkout command must be an object");
  }

  if (typeof command.item !== "string" || command.item.trim().length === 0) {
    throw new TypeError("item must be a non-empty string");
  }

  if (!Number.isSafeInteger(command.quantity) || command.quantity <= 0) {
    throw new TypeError("quantity must be a positive safe integer");
  }

  if (typeof command.amount !== "number" || !Number.isFinite(command.amount) || command.amount <= 0) {
    throw new TypeError("amount must be a positive finite number");
  }

  if (
    command.simulateFailureAt !== undefined &&
    command.simulateFailureAt !== null &&
    !SUPPORTED_FAILURE_POINTS.has(command.simulateFailureAt)
  ) {
    throw new TypeError(
      `simulateFailureAt must be one of: ${Array.from(SUPPORTED_FAILURE_POINTS).join(", ")}`
    );
  }
}

function validateSagaContext(context) {
  validateCheckoutCommand(context);

  for (const fieldName of ["aggregateId", "reservationId", "paymentId"]) {
    if (!isIdentifier(context[fieldName])) {
      throw new TypeError(`${fieldName} must be a non-empty string or a positive safe integer`);
    }
  }
}

function assertDependencies({ recordEvent, getState } = {}) {
  if (typeof recordEvent !== "function") {
    throw new TypeError("recordEvent must be a function");
  }

  if (typeof getState !== "function") {
    throw new TypeError("getState must be a function");
  }
}

function createSimulatedFailure(failurePoint) {
  const error = new Error(`Simulated checkout failure at ${failurePoint}`);
  error.code = "SIMULATED_CHECKOUT_FAILURE";
  error.failurePoint = failurePoint;
  return error;
}

function failIfRequested(requestedFailurePoint, currentFailurePoint) {
  if (requestedFailurePoint === currentFailurePoint) {
    throw createSimulatedFailure(currentFailurePoint);
  }
}

function isCompensatableCheckoutFailure(error) {
  return error?.code === "SIMULATED_CHECKOUT_FAILURE";
}

function compensateCheckout(
  { reason = "Checkout compensation requested" } = {},
  dependencies
) {
  assertDependencies(dependencies);

  if (typeof reason !== "string" || reason.trim().length === 0) {
    throw new TypeError("compensation reason must be a non-empty string");
  }

  const { recordEvent, getState } = dependencies;
  const compensationEvents = [];
  let state = getState();

  if (!state || state.deleted) {
    return compensationEvents;
  }

  if (state.payment?.status === "charged") {
    compensationEvents.push(
      recordEvent(EVENT_TYPES.PAYMENT_REFUNDED, {
        paymentId: state.payment.id,
        reason,
      })
    );
    state = getState();
  }

  if (state.inventory?.status === "reserved") {
    compensationEvents.push(
      recordEvent(EVENT_TYPES.INVENTORY_RELEASED, {
        reservationId: state.inventory.id,
        reason,
      })
    );
    state = getState();
  }

  if (state.order && state.order.status !== "rolled_back") {
    compensationEvents.push(
      recordEvent(EVENT_TYPES.ORDER_ROLLED_BACK, {
        reason,
      })
    );
  }

  return compensationEvents;
}

function runCheckoutSaga(context, dependencies) {
  validateSagaContext(context);
  assertDependencies(dependencies);

  const { recordEvent, getState } = dependencies;
  const completedSteps = [];
  const forwardEvents = [];

  try {
    forwardEvents.push(
      recordEvent(EVENT_TYPES.ORDER_CREATED, {
        item: context.item,
        quantity: context.quantity,
      })
    );
    completedSteps.push(EVENT_TYPES.ORDER_CREATED);
    failIfRequested(context.simulateFailureAt, FAILURE_POINTS.AFTER_ORDER);

    forwardEvents.push(
      recordEvent(EVENT_TYPES.INVENTORY_RESERVED, {
        reservationId: context.reservationId,
        item: context.item,
        quantity: context.quantity,
      })
    );
    completedSteps.push(EVENT_TYPES.INVENTORY_RESERVED);
    failIfRequested(context.simulateFailureAt, FAILURE_POINTS.AFTER_INVENTORY);

    forwardEvents.push(
      recordEvent(EVENT_TYPES.PAYMENT_CHARGED, {
        paymentId: context.paymentId,
        amount: context.amount,
      })
    );
    completedSteps.push(EVENT_TYPES.PAYMENT_CHARGED);
    failIfRequested(context.simulateFailureAt, FAILURE_POINTS.AFTER_PAYMENT);

    return {
      aggregateId: context.aggregateId,
      status: "completed",
      failedAt: null,
      error: null,
      completedSteps,
      forwardEvents,
      compensationEvents: [],
      events: [...forwardEvents],
    };
  } catch (error) {
    if (!isCompensatableCheckoutFailure(error)) {
      throw error;
    }

    const compensationEvents = compensateCheckout(
      { reason: error.message },
      { recordEvent, getState }
    );
    const finalState = getState();

    return {
      aggregateId: context.aggregateId,
      status: finalState?.lifecycle === "rolled_back" ? "rolled_back" : "failed",
      failedAt: error.failurePoint || context.simulateFailureAt || null,
      error: error.message,
      completedSteps,
      forwardEvents,
      compensationEvents,
      events: [...forwardEvents, ...compensationEvents],
    };
  }
}

module.exports = {
  FAILURE_POINTS,
  compensateCheckout,
  runCheckoutSaga,
  validateCheckoutCommand,
};
