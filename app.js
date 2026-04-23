const express = require("express");
const app = express();

app.use(express.json());

// In-memory order store
const orders = new Map();
let orderCounter = 1;

// In-memory checkout stores
const inventoryReservations = new Map();
const payments = new Map();
let reservationCounter = 1;
let paymentCounter = 1;

// History for rollback engine
const history = [];
const snapshots = new Map();

function addHistoryEntry(action) {
  const enrichedAction = {
    ...action,
    step: action.step,
    entityId: action.entityId,
    entity: action.entity ? { ...action.entity } : action.entity,
    timestamp: action.timestamp || new Date().toISOString(),
  };

  history.push(enrichedAction);
  console.log(`🧾 HISTORY: Added ${enrichedAction.type} (${enrichedAction.step}) entityId=${enrichedAction.entityId}`);

  return enrichedAction;
}

function rollbackCheckout(checkoutHistory) {
  const rollbackResults = [];

  console.log("↩️ Starting checkout rollback...");

  for (const action of [...checkoutHistory].reverse()) {
    if (action.type === "CHARGE_PAYMENT") {
      const payment = payments.get(action.paymentId);

      if (!payment) {
        console.log(`↩️ ROLLBACK PAYMENT: Payment ID=${action.paymentId}, Order ID=${action.orderId} not found`);
        rollbackResults.push({
          step: "payment",
          action: "refund",
          status: "not_found",
          entity: null,
        });
        continue;
      }

      if (payment.status === "refunded") {
        console.log(`↩️ ROLLBACK PAYMENT: Payment ID=${action.paymentId}, Order ID=${action.orderId} already rolled back`);
        rollbackResults.push({
          step: "payment",
          action: "refund",
          status: "already_rolled_back",
          entity: payment,
        });
        continue;
      }

      payment.status = "refunded";
      payment.refundedAt = new Date().toISOString();

      console.log(`↩️ ROLLBACK PAYMENT: Refunded payment ID=${action.paymentId}, Order ID=${action.orderId}`);
      rollbackResults.push({
        step: "payment",
        action: "refund",
        status: "rolled_back",
        entity: payment,
      });
    }

    if (action.type === "RESERVE_INVENTORY") {
      const reservation = inventoryReservations.get(action.reservationId);

      if (!reservation) {
        console.log(`↩️ ROLLBACK INVENTORY: Reservation ID=${action.reservationId}, Order ID=${action.orderId} not found`);
        rollbackResults.push({
          step: "inventory",
          action: "release",
          status: "not_found",
          entity: null,
        });
        continue;
      }

      if (reservation.status === "released") {
        console.log(`↩️ ROLLBACK INVENTORY: Reservation ID=${action.reservationId}, Order ID=${action.orderId} already rolled back`);
        rollbackResults.push({
          step: "inventory",
          action: "release",
          status: "already_rolled_back",
          entity: reservation,
        });
        continue;
      }

      reservation.status = "released";
      reservation.releasedAt = new Date().toISOString();

      console.log(`↩️ ROLLBACK INVENTORY: Released reservation ID=${action.reservationId}, Order ID=${action.orderId}`);
      rollbackResults.push({
        step: "inventory",
        action: "release",
        status: "rolled_back",
        entity: reservation,
      });
    }

    if (action.type === "CREATE_ORDER") {
      const order = orders.get(action.orderId);

      if (!order) {
        console.log(`↩️ ROLLBACK ORDER: Order ID=${action.orderId} not found`);
        rollbackResults.push({
          step: "order",
          action: "mark_rolled_back",
          status: "not_found",
          entity: null,
        });
        continue;
      }

      if (order.status === "rolled_back") {
        console.log(`↩️ ROLLBACK ORDER: Order ID=${action.orderId} already rolled back`);
        rollbackResults.push({
          step: "order",
          action: "mark_rolled_back",
          status: "already_rolled_back",
          entity: order,
        });
        continue;
      }

      order.status = "rolled_back";
      order.rolledBackAt = new Date().toISOString();

      console.log(`↩️ ROLLBACK ORDER: Rolled back order ID=${action.orderId}`);
      rollbackResults.push({
        step: "order",
        action: "mark_rolled_back",
        status: "rolled_back",
        entity: order,
      });
    }
  }

  console.log("↩️ Checkout rollback complete.");
  return rollbackResults;
}

function replayOrder(orderId) {
  const numericOrderId = Number(orderId);
  const orderHistory = history
    .filter((action) => Number(action.orderId) === numericOrderId || Number(action.entityId) === numericOrderId)
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  const results = [];

  console.log(`🔁 REPLAY: Starting replay for order ID=${numericOrderId}`);
  console.log(`🔁 REPLAY DEBUG: Found ${orderHistory.length} history entries for order ID=${numericOrderId}`);
  console.log("🔁 REPLAY DEBUG: Filtered entries:", orderHistory);

  for (const action of orderHistory) {
    if (action.type === "CREATE_ORDER") {
      if (orders.has(action.orderId)) {
        console.log(`🔁 REPLAY ORDER: Order ID=${action.orderId} already exists`);
        results.push({ step: "order", status: "already_exists" });
        continue;
      }

      console.log(`🔁 REPLAY ORDER: Cannot reconstruct order ID=${action.orderId}; history does not contain enough source data`);
      console.log(`🔁 REPLAY ORDER: Skipped replay for order ID=${action.orderId}`);
      results.push({ step: "order", status: "cannot_reconstruct" });
    }

    if (action.type === "RESERVE_INVENTORY") {
      if (inventoryReservations.has(action.reservationId)) {
        console.log(`🔁 REPLAY INVENTORY: Reservation ID=${action.reservationId} already exists`);
        results.push({ step: "inventory", status: "already_exists" });
        continue;
      }

      console.log(`🔁 REPLAY INVENTORY: Cannot reconstruct reservation ID=${action.reservationId}; history does not contain enough source data`);
      console.log(`🔁 REPLAY INVENTORY: Skipped replay for reservation ID=${action.reservationId}, Order ID=${action.orderId}`);
      results.push({ step: "inventory", status: "cannot_reconstruct" });
    }

    if (action.type === "CHARGE_PAYMENT") {
      if (payments.has(action.paymentId)) {
        console.log(`🔁 REPLAY PAYMENT: Payment ID=${action.paymentId} already exists`);
        results.push({ step: "payment", status: "already_exists" });
        continue;
      }

      console.log(`🔁 REPLAY PAYMENT: Cannot reconstruct payment ID=${action.paymentId}; history does not contain enough source data`);
      console.log(`🔁 REPLAY PAYMENT: Skipped replay for payment ID=${action.paymentId}, Order ID=${action.orderId}`);
      results.push({ step: "payment", status: "cannot_reconstruct" });
    }
  }

  console.log(`🔁 REPLAY: Complete for order ID=${numericOrderId}, steps=${results.length}`);

  return {
    orderId: numericOrderId,
    stepsReplayed: results.length,
    results,
  };
}

function applyEventToState(state, event) {
  if (event.type === "CREATE_ORDER") {
    state.order = event.entity;
  }

  if (event.type === "RESERVE_INVENTORY") {
    state.inventory = event.entity;
  }

  if (event.type === "CHARGE_PAYMENT") {
    state.payment = event.entity;
  }
}

function createSnapshotFromHistory(orderId, checkoutHistory) {
  const lastEvent = checkoutHistory[checkoutHistory.length - 1];

  if (!lastEvent) {
    return null;
  }

  const state = {
    order: null,
    inventory: null,
    payment: null,
  };

  for (const event of checkoutHistory) {
    applyEventToState(state, event);
  }

  const snapshot = {
    orderId,
    state,
    lastEventTimestamp: lastEvent.timestamp,
  };

  snapshots.set(orderId, snapshot);
  console.log("📸 SNAPSHOT CREATED:", snapshot);

  return snapshot;
}

function rebuildStateFromHistory(orderId) {
  const numericOrderId = Number(orderId);
  const snapshot = snapshots.get(numericOrderId);
  let orderHistory = history
    .filter((action) => Number(action.orderId) === numericOrderId || Number(action.entityId) === numericOrderId)
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  let order = null;
  let inventory = null;
  let payment = null;

  console.log(`📜 EVENT SOURCING: Rebuilding state for order ID=${numericOrderId}`);

  if (snapshot) {
    console.log(`📸 SNAPSHOT: Using snapshot for order ${numericOrderId}`);
    order = snapshot.state.order;
    inventory = snapshot.state.inventory;
    payment = snapshot.state.payment;
    orderHistory = orderHistory.filter(
      (event) => new Date(event.timestamp).getTime() > new Date(snapshot.lastEventTimestamp).getTime()
    );
    console.log(`📸 SNAPSHOT: Replaying ${orderHistory.length} events after snapshot`);
  }

  const state = { order, inventory, payment };

  for (const event of orderHistory) {
    console.log(`📜 EVENT SOURCING: Applying ${event.type}`, event);
    applyEventToState(state, event);
  }

  console.log(`📜 EVENT SOURCING: Final reconstructed state for order ID=${numericOrderId}`, state);

  return state;
}

function rebuildStateAtTimestamp(orderId, timestamp) {
  const numericOrderId = Number(orderId);
  const targetTimestamp = new Date(timestamp);
  const targetTime = targetTimestamp.getTime();
  const events = history
    .filter((event) => {
      const eventOrderIdMatches = Number(event.orderId) === numericOrderId || Number(event.entityId) === numericOrderId;
      const eventTime = new Date(event.timestamp).getTime();
      const passedFilter = eventOrderIdMatches && eventTime <= targetTime;

      console.log(
        `⏱️ TIME TRAVEL DEBUG: event=${event.type}, eventTimestamp=${event.timestamp}, inputTimestamp=${timestamp}, orderMatch=${eventOrderIdMatches}, passed=${passedFilter}`
      );

      return passedFilter;
    })
    .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
  const state = {
    order: null,
    inventory: null,
    payment: null,
  };

  console.log(`⏱️ TIME TRAVEL: Rebuilding state at timestamp ${timestamp} for order ID=${numericOrderId}`);
  console.log(`⏱️ TIME TRAVEL DEBUG: Input timestamp=${timestamp}, parsedMs=${targetTime}`);
  console.log(`⏱️ TIME TRAVEL: Using ${events.length} events`);

  for (const event of events) {
    console.log(`⏱️ TIME TRAVEL: Applying ${event.type}`, event);
    applyEventToState(state, event);
  }

  console.log(`⏱️ TIME TRAVEL: Final reconstructed state for order ID=${numericOrderId}`, state);

  return state;
}

function restoreStateFromHistory(orderId) {
  const numericOrderId = Number(orderId);
  const state = rebuildStateFromHistory(numericOrderId);

  console.log("♻️ RESTORE:", state);

  if (state.order) {
    if (orders.has(state.order.id)) {
      console.log(`♻️ RESTORE: Order ID=${state.order.id} already exists`);
    } else {
      orders.set(state.order.id, state.order);
      console.log(`♻️ RESTORE: Restored order ID=${state.order.id}`);
    }
  }

  if (state.inventory) {
    if (inventoryReservations.has(state.inventory.id)) {
      console.log(`♻️ RESTORE: Inventory reservation ID=${state.inventory.id} already exists`);
    } else {
      inventoryReservations.set(state.inventory.id, state.inventory);
      console.log(`♻️ RESTORE: Restored inventory reservation ID=${state.inventory.id}`);
    }
  }

  if (state.payment) {
    if (payments.has(state.payment.id)) {
      console.log(`♻️ RESTORE: Payment ID=${state.payment.id} already exists`);
    } else {
      payments.set(state.payment.id, state.payment);
      console.log(`♻️ RESTORE: Restored payment ID=${state.payment.id}`);
    }
  }

  return {
    orderId: numericOrderId,
    restored: true,
    state,
  };
}

// Test Route
app.get("/", (req, res) => {
  res.send("Rollback Engine läuft 😈");
});

// POST /order - Create a new order
app.post("/order", (req, res) => {
  const { item } = req.body;
  
  if (!item) {
    return res.status(400).json({ error: "Item is required" });
  }
  
  const orderId = orderCounter++;
  const order = { id: orderId, item, createdAt: new Date().toISOString() };
  orders.set(orderId, order);
  
  // Track action in history
  addHistoryEntry({
    type: "CREATE_ORDER",
    step: "order",
    entityId: orderId,
    orderId,
    entity: { ...order },
  });
  
  console.log(`✅ Order created: ID=${orderId}, Item=${item}`);
  res.status(201).json(order);
});

// DELETE /order/:id - Delete an order (undo)
app.delete("/order/:id", (req, res) => {
  const orderId = parseInt(req.params.id);
  
  if (!orders.has(orderId)) {
    return res.status(404).json({ error: "Order not found" });
  }
  
  const order = orders.get(orderId);
  orders.delete(orderId);
  
  // Track action in history
  addHistoryEntry({
    type: "DELETE_ORDER",
    step: "order",
    entityId: orderId,
    orderId,
    entity: { ...order },
  });
  
  console.log(`🗑️ Order deleted: ID=${orderId}, Item=${order.item}`);
  res.json({ message: "Order deleted", order });
});

// GET /orders - View all orders
app.get("/orders", (req, res) => {
  const allOrders = Array.from(orders.values());
  res.json(allOrders);
});

// GET /history - View full Saga action history
app.get("/history", (req, res) => {
  console.log(`🧾 HISTORY: Requested full history, count=${history.length}`);
  res.json({
    count: history.length,
    history,
  });
});

// POST /replay/:orderId - Replay a workflow from history
app.post("/replay/:orderId", (req, res) => {
  const orderId = Number(req.params.orderId);
  const replay = replayOrder(orderId);

  res.json(replay);
});

// GET /replay-state/:orderId - Rebuild state from event history
app.get("/replay-state/:orderId", (req, res) => {
  const orderId = Number(req.params.orderId);
  const state = rebuildStateFromHistory(orderId);

  res.json(state);
});

// GET /snapshot/:orderId - View stored state snapshot
app.get("/snapshot/:orderId", (req, res) => {
  const orderId = Number(req.params.orderId);
  const snapshot = snapshots.get(orderId);

  res.json({ snapshot });
});

// GET /state-at/:orderId/:timestamp - Rebuild state at a specific timestamp
app.get("/state-at/:orderId/:timestamp", (req, res) => {
  const orderId = Number(req.params.orderId);
  const state = rebuildStateAtTimestamp(orderId, req.params.timestamp);

  res.json(state);
});

// POST /replay-restore/:orderId - Restore in-memory state from event history
app.post("/replay-restore/:orderId", (req, res) => {
  const orderId = Number(req.params.orderId);
  const restore = restoreStateFromHistory(orderId);

  res.json(restore);
});

app.post("/checkout", (req, res) => {
  const { item, quantity = 1, amount = 100 } = req.body;
  const checkoutHistory = [];

  if (!item) {
    return res.status(400).json({ error: "Item is required" });
  }

  try {
    const orderId = orderCounter++;
    const order = {
      id: orderId,
      item,
      quantity,
      status: "created",
      createdAt: new Date().toISOString(),
    };
    orders.set(orderId, order);

    const createOrderAction = {
      type: "CREATE_ORDER",
      step: "order",
      entityId: orderId,
      orderId,
      entity: { ...order },
    };
    const createOrderHistoryEntry = addHistoryEntry(createOrderAction);
    checkoutHistory.push(createOrderHistoryEntry);
    console.log(`✅ CHECKOUT: Created order ID=${orderId}, Item=${item}`);

    const reservationId = reservationCounter++;
    const reservation = {
      id: reservationId,
      orderId,
      item,
      quantity,
      status: "reserved",
      createdAt: new Date().toISOString(),
    };
    inventoryReservations.set(reservationId, reservation);

    const reserveInventoryAction = {
      type: "RESERVE_INVENTORY",
      step: "inventory",
      entityId: reservationId,
      reservationId,
      orderId,
      entity: { ...reservation },
    };
    const reserveInventoryHistoryEntry = addHistoryEntry(reserveInventoryAction);
    checkoutHistory.push(reserveInventoryHistoryEntry);
    console.log(`📦 CHECKOUT: Reserved inventory ID=${reservationId}, Order ID=${orderId}`);

    const paymentId = paymentCounter++;
    const payment = {
      id: paymentId,
      orderId,
      amount,
      status: "charged",
      createdAt: new Date().toISOString(),
    };
    payments.set(paymentId, payment);

    const chargePaymentAction = {
      type: "CHARGE_PAYMENT",
      step: "payment",
      entityId: paymentId,
      paymentId,
      orderId,
      entity: { ...payment },
    };
    const chargePaymentHistoryEntry = addHistoryEntry(chargePaymentAction);
    checkoutHistory.push(chargePaymentHistoryEntry);
    console.log(`💳 CHECKOUT: Charged payment ID=${paymentId}, Order ID=${orderId}`);

    createSnapshotFromHistory(orderId, checkoutHistory);

    throw new Error("Simulated checkout failure after payment");
  } catch (error) {
    console.log(`❌ CHECKOUT FAILED: ${error.message}`);
    const rollback = rollbackCheckout(checkoutHistory);

    return res.status(500).json({
      error: error.message,
      rolledBack: true,
      rollback,
    });
  }
});

app.listen(3000, () => {
  console.log("Server läuft auf http://localhost:3000");
});
