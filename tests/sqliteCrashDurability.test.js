const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { spawnSync } = require("node:child_process");

const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { EVENT_TYPES } = require("../src/domain/events");
const { FAILURE_POINTS } = require("../src/domain/checkoutSaga");

function createEngineWithDbPath(dbPath) {
  const adapters = createStorageAdapters({ type: "sqlite", dbPath });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
  });

  return { engine, adapters };
}

test("state and events survive process restart in separate Node child process", () => {
  const dbPath = join(tmpdir(), `rollback-crash-${randomUUID()}.db`);
  const idempotencyKey = "crash-key-1";

  // Step 1: Execute in a separate Node.js child process
  const childScript1 = `
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { RollbackEngine } = require('./src/application/rollbackEngine');

    const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
    const engine = new RollbackEngine({
      eventStore: adapters.eventStore,
      commandStore: adapters.commandStore,
      snapshotStore: adapters.snapshotStore,
      stateRepository: adapters.stateRepository,
    });

    const result = engine.checkout(
      { item: 'ServerRack', quantity: 2, amount: 1500 },
      { commandId: ${JSON.stringify(idempotencyKey)} }
    );

    adapters.close();
    process.stdout.write(JSON.stringify({ aggregateId: result.aggregateId, status: result.status }));
  `;

  const child1 = spawnSync(process.execPath, ["-e", childScript1], {
    cwd: process.cwd(),
    encoding: "utf8",
  });

  assert.equal(child1.status, 0, child1.stderr);
  const child1Output = JSON.parse(child1.stdout);
  assert.equal(child1Output.status, "completed");
  const aggregateId = child1Output.aggregateId;

  // Step 2: Open database in a fresh process / new engine instance and verify full survival
  const { engine: restartEngine, adapters: restartAdapters } = createEngineWithDbPath(dbPath);

  try {
    const events = restartEngine.getEvents(aggregateId);
    assert.equal(events.length, 3);
    assert.deepEqual(
      events.map((e) => e.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
      ]
    );

    // Verify replay equals live state
    const replayed = restartEngine.replay(aggregateId);
    assert.equal(replayed.lifecycle, "completed");
    assert.equal(replayed.order.item, "ServerRack");
    assert.equal(replayed.order.quantity, 2);
    assert.equal(replayed.payment.amount, 1500);

    // Verify snapshot was persisted and survives
    const snapshot = restartEngine.getSnapshot(aggregateId);
    assert.notEqual(snapshot, null);
    assert.equal(snapshot.version, 3);
    assert.deepEqual(snapshot.state, replayed);

    // Verify idempotent deduplication after restart: same key + payload returns original result without new events
    const retryResult = restartEngine.checkout(
      { item: 'ServerRack', quantity: 2, amount: 1500 },
      { commandId: idempotencyKey }
    );
    assert.equal(retryResult.aggregateId, aggregateId);
    assert.equal(retryResult.status, "completed");
    assert.equal(restartEngine.getEvents(aggregateId).length, 3);
  } finally {
    restartAdapters.close();
  }
});

test("compensated saga persists 6 events and recovers rollback state across restart", () => {
  const dbPath = join(tmpdir(), `rollback-saga-${randomUUID()}.db`);
  const idempotencyKey = "saga-fail-key-1";

  // Step 1: Run in child process with failure after payment
  const childScript = `
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { RollbackEngine } = require('./src/application/rollbackEngine');

    const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
    const engine = new RollbackEngine({
      eventStore: adapters.eventStore,
      commandStore: adapters.commandStore,
      snapshotStore: adapters.snapshotStore,
      stateRepository: adapters.stateRepository,
    });

    const result = engine.checkout(
      { item: 'Smartphone', quantity: 1, amount: 800, simulateFailureAt: 'after_payment' },
      { commandId: ${JSON.stringify(idempotencyKey)} }
    );

    adapters.close();
    process.stdout.write(JSON.stringify({ aggregateId: result.aggregateId, status: result.status }));
  `;

  const child = spawnSync(process.execPath, ["-e", childScript], {
    cwd: process.cwd(),
    encoding: "utf8",
  });

  assert.equal(child.status, 0, child.stderr);
  const { aggregateId, status } = JSON.parse(child.stdout);
  assert.equal(status, "rolled_back");

  // Step 2: In new process, verify exact event sequence
  const { engine: restartEngine, adapters } = createEngineWithDbPath(dbPath);

  try {
    const events = restartEngine.getEvents(aggregateId);
    assert.equal(events.length, 6);
    assert.deepEqual(
      events.map((e) => e.eventType),
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
      events.map((e) => e.sequence),
      [1, 2, 3, 4, 5, 6]
    );

    const replayed = restartEngine.replay(aggregateId);
    assert.equal(replayed.lifecycle, "rolled_back");
    assert.equal(replayed.payment.status, "refunded");
    assert.equal(replayed.inventory.status, "released");
    assert.equal(replayed.order.status, "rolled_back");

    // Idempotent retry returns rolled_back result without new events
    const retryResult = restartEngine.checkout(
      { item: 'Smartphone', quantity: 1, amount: 800, simulateFailureAt: 'after_payment' },
      { commandId: idempotencyKey }
    );
    assert.equal(retryResult.status, "rolled_back");
    assert.equal(restartEngine.getEvents(aggregateId).length, 6);
  } finally {
    adapters.close();
  }
});

test("lost ACK scenario: command committed event but crashed before complete is reconciled on restart", () => {
  const dbPath = join(tmpdir(), `rollback-lostack-${randomUUID()}.db`);
  const idempotencyKey = "lost-ack-cmd-1";
  const db = createSqliteDatabase({ path: dbPath });

  try {
    // Manually simulate crash right after event was committed but before command was marked completed:
    // 1. Command record is in 'processing'
    db.prepare(`
      INSERT INTO commands (command_id, command_type, payload, status)
      VALUES (?, ?, ?, 'processing')
    `).run(idempotencyKey, "CREATE_ORDER", JSON.stringify({ item: "GPU", quantity: 1 }));

    // 2. Event Store actually committed the event!
    db.prepare(`
      INSERT INTO events (event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "evt-lost-1",
      42,
      1,
      idempotencyKey,
      "ORDER_CREATED",
      new Date().toISOString(),
      JSON.stringify({ item: "GPU", quantity: 1 }),
      JSON.stringify({ schemaVersion: 1, commandId: idempotencyKey, correlationId: idempotencyKey, causationId: idempotencyKey })
    );
  } finally {
    db.close();
  }

  // New process boots up and receives retry of same command
  const { engine, adapters } = createEngineWithDbPath(dbPath);

  try {
    // Reconciles existing commit and throws interrupted command error to prevent duplicate execution
    assert.throws(
      () => engine.createOrder({ item: "GPU", quantity: 1 }, { commandId: idempotencyKey }),
      (err) => {
        assert.equal(err.code, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");
        assert.equal(err.eventCommitted, true);
        assert.equal(err.retrySafe, false);
        assert.equal(err.retryAction, "MANUAL_RESOLUTION_REQUIRED");
        return true;
      }
    );

    // Event store was not modified, still exactly 1 event
    assert.equal(engine.getEvents(42).length, 1);
  } finally {
    adapters.close();
  }
});

test("self-heals corrupted materialized state from persistent event log across restart", () => {
  const dbPath = join(tmpdir(), `rollback-selfheal-${randomUUID()}.db`);
  const { engine: initEngine, adapters: initAdapters } = createEngineWithDbPath(dbPath);

  let aggregateId;
  try {
    const checkout = initEngine.checkout({ item: "Laptop", quantity: 1, amount: 2000 });
    aggregateId = checkout.aggregateId;
  } finally {
    initAdapters.close();
  }

  // Simulate direct SQLite disk corruption of the materialized view table
  const rawDb = createSqliteDatabase({ path: dbPath });
  try {
    rawDb.prepare("UPDATE materialized_states SET state = ? WHERE aggregate_id = ?").run(
      JSON.stringify({ aggregateId, version: 3, order: { item: "CORRUPTED_CACHE", quantity: 999 } }),
      aggregateId
    );
  } finally {
    rawDb.close();
  }

  // Fresh engine instance starts
  const { engine: restartEngine, adapters: restartAdapters } = createEngineWithDbPath(dbPath);

  try {
    // Materialized consistency initially sees the corrupted view
    const matOrder = restartEngine.getOrder(aggregateId, { consistency: "materialized" });
    assert.equal(matOrder.item, "CORRUPTED_CACHE");

    // Authoritative read detects drift against authoritative event stream and self-heals
    const authOrder = restartEngine.getOrder(aggregateId, { consistency: "authoritative" });
    assert.equal(authOrder.item, "Laptop");
    assert.equal(authOrder.quantity, 1);

    // Verify SQLite materialized_states table on disk was repaired
    const repairedMatOrder = restartEngine.getOrder(aggregateId, { consistency: "materialized" });
    assert.equal(repairedMatOrder.item, "Laptop");
  } finally {
    restartAdapters.close();
  }
});

test("boundary test: processing command with 0 committed events is NOT stolen or automatically reset on restart", () => {
  const dbPath = join(tmpdir(), `rollback-processing0-${randomUUID()}.db`);
  const commandId = "processing-zero-events-key";
  const rawDb = createSqliteDatabase({ path: dbPath });

  try {
    // Persist a processing command without any committed events (simulating process death before first append)
    rawDb.prepare(`
      INSERT INTO commands (command_id, command_type, payload, status)
      VALUES (?, ?, ?, 'processing')
    `).run(commandId, "CHECKOUT", JSON.stringify({ item: "Bike", quantity: 1, amount: 500, simulateFailureAt: null }));
  } finally {
    rawDb.close();
  }

  // Start fresh process/engine
  const { engine, adapters } = createEngineWithDbPath(dbPath);

  try {
    // Verify that the command is NOT automatically stolen or executed again.
    // It remains in progress and returns COMMAND_IN_PROGRESS to protect in-flight worker uncertainty.
    assert.throws(
      () => engine.checkout({ item: "Bike", quantity: 1, amount: 500 }, { commandId }),
      (err) => {
        assert.equal(err.code, "COMMAND_IN_PROGRESS");
        assert.equal(err.eventCommitted, false);
        assert.equal(err.retrySafe, false);
        assert.equal(err.retryAction, "WAIT_AND_RETRY_SAME_KEY");
        return true;
      }
    );

    // Event Store remains clean (0 events)
    assert.equal(engine.getAllEvents().length, 0);
  } finally {
    adapters.close();
  }
});
