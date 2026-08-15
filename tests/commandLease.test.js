const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { InMemoryCommandStore } = require("../src/infrastructure/inMemoryCommandStore");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");
const { SqliteCommandStore } = require("../src/infrastructure/sqlite/sqliteCommandStore");
const { SqliteEventStore } = require("../src/infrastructure/sqlite/sqliteEventStore");
const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");

function createEngineWithCustomClock({
  storageType = "memory",
  dbPath = ":memory:",
  workerId = "worker-1",
  leaseTtlMs = 1000,
  getNow,
  diagnostics = [],
} = {}) {
  const adapters = createStorageAdapters({ type: storageType, dbPath, now: getNow });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    workerId,
    leaseTtlMs,
    now: getNow,
    diagnosticReporter: (d) => diagnostics.push(d),
  });

  return { engine, adapters, diagnostics };
}

test("command lease: initial reservation acquires lease with token 1 and owner", () => {
  let currentTime = 1000;
  const diagnostics = [];
  const { engine, adapters } = createEngineWithCustomClock({
    workerId: "worker-alpha",
    leaseTtlMs: 2000,
    getNow: () => currentTime,
    diagnostics,
  });

  const commandId = "lease-test-1";
  const result = engine.checkout(
    { item: "Laptop", quantity: 1, amount: 1200 },
    { commandId }
  );

  assert.equal(result.status, "completed");
  const storedCmd = adapters.commandStore.get(commandId);
  assert.equal(storedCmd.status, "completed");
  assert.equal(storedCmd.leaseToken, 1);
  assert.equal(storedCmd.leaseOwner, null); // cleared on complete
  assert.equal(storedCmd.leaseExpiresAt, null);
});

test("command lease: active unexpired command with 0 events returns COMMAND_IN_PROGRESS", () => {
  let currentTime = 1000;
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore({ commandStore, now: () => currentTime });
  const commandId = "in-flight-command";

  // Worker 1 reserves command with lease expiring at 3000
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Bike", quantity: 1, amount: 500, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 2000,
    now: currentTime,
  });

  // Worker 2 attempts execution at currentTime = 2000 (lease still valid until 3000)
  currentTime = 2000;
  const engineWorker2 = new RollbackEngine({
    commandStore,
    eventStore,
    workerId: "worker-2",
    now: () => currentTime,
  });

  assert.throws(
    () =>
      engineWorker2.checkout(
        { item: "Bike", quantity: 1, amount: 500 },
        { commandId }
      ),
    (err) => {
      assert.equal(err.code, "COMMAND_IN_PROGRESS");
      assert.equal(err.commandId, commandId);
      assert.equal(err.eventCommitted, false);
      assert.equal(err.retrySafe, false);
      assert.equal(err.retryAction, "WAIT_AND_RETRY_SAME_KEY");
      return true;
    }
  );

  assert.equal(eventStore.getAll().length, 0);
});

test("command lease: expired command with 0 events is safely taken over by new worker with incremented fencing token", () => {
  let currentTime = 1000;
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore({ commandStore, now: () => currentTime });
  const commandId = "abandoned-processing-command";
  const diagnostics = [];

  // Worker 1 reserves at t=1000 with 2000ms TTL (expires at 3000)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Phone", quantity: 1, amount: 800, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 2000,
    now: currentTime,
  });

  // Time advances to t=3500 (lease is expired, 0 events committed)
  currentTime = 3500;
  const engineWorker2 = new RollbackEngine({
    commandStore,
    eventStore,
    workerId: "worker-2",
    leaseTtlMs: 3000,
    now: () => currentTime,
    diagnosticReporter: (d) => diagnostics.push(d),
  });

  // Worker 2 should take over and successfully execute
  const result = engineWorker2.checkout(
    { item: "Phone", quantity: 1, amount: 800 },
    { commandId }
  );

  assert.equal(result.status, "completed");
  assert.equal(eventStore.getAll().length, 3);

  // Stored command should reflect successful completion with token 2
  const storedCmd = commandStore.get(commandId);
  assert.equal(storedCmd.status, "completed");
  assert.equal(storedCmd.leaseToken, 2);

  // Verify diagnostic emission for lease takeover
  const takeoverDiagnostic = diagnostics.find(
    (d) => d.type === "COMMAND_LEASE" && d.status === "LEASE_TAKEN_OVER"
  );
  assert.notEqual(takeoverDiagnostic, undefined);
  assert.equal(takeoverDiagnostic.workerId, "worker-2");
  assert.equal(takeoverDiagnostic.fencingToken, 2);
  assert.equal(takeoverDiagnostic.previousToken, 1);
});

test("fencing: zombie worker with stale fencing token is rejected at EventStore.append", () => {
  let currentTime = 1000;
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore({ commandStore, now: () => currentTime });
  const commandId = "zombie-test-command";

  // Worker 1 reserves at t=1000 (token 1, expires at 2000)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Tablet", quantity: 1, amount: 400, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 1000,
    now: currentTime,
  });

  // Worker 2 takes over at t=2500 (token becomes 2, expires at 4500)
  currentTime = 2500;
  const takeover = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 2000,
    now: currentTime,
  });
  assert.equal(takeover.success, true);
  assert.equal(takeover.record.leaseToken, 2);

  // Worker 1 (zombie) attempts to append an event using stale token 1
  const staleEvent = createDomainEvent({
    eventId: "zombie-event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: "Tablet", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });

  assert.throws(
    () => eventStore.append(staleEvent, { expectedVersion: 0, fencingToken: 1 }),
    (err) => {
      assert.equal(err.code, "FENCING_TOKEN_STALE");
      assert.equal(err.commandId, commandId);
      assert.equal(err.providedToken, 1);
      assert.equal(err.currentToken, 2);
      assert.equal(err.eventCommitted, false);
      assert.equal(err.retrySafe, false);
      return true;
    }
  );

  // Verify EventStore has 0 events appended by the zombie
  assert.equal(eventStore.getAll().length, 0);

  // Worker 2 appends valid event with token 2
  const validEvent = createDomainEvent({
    eventId: "valid-event-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: "Tablet", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });

  const appended = eventStore.append(validEvent, { expectedVersion: 0, fencingToken: 2 });
  assert.equal(appended.eventId, "valid-event-1");
  assert.equal(eventStore.getAll().length, 1);
});

test("SQLite atomic fencing check rejects stale append in shared database", () => {
  const dbPath = join(tmpdir(), `rollback-fencing-${randomUUID()}.db`);
  const db = createSqliteDatabase({ path: dbPath });

  try {
    const commandStore = new SqliteCommandStore({ db });
    const eventStore = new SqliteEventStore({ db, now: () => 2500 });
    const commandId = "sqlite-fencing-cmd";

    // Worker 1 reserves command with token 1
    commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: { item: "Monitor", quantity: 1, amount: 300 },
      workerId: "worker-1",
      leaseTtlMs: 1000,
      now: 1000,
    });

    // Worker 2 takes over after expiry at t=2500 -> token becomes 2
    const takeover = commandStore.takeOverExpired({
      commandId,
      workerId: "worker-2",
      leaseTtlMs: 2000,
      now: 2500,
    });
    assert.equal(takeover.success, true);
    assert.equal(takeover.record.leaseToken, 2);

    // Stale Worker 1 attempts append with token 1
    const staleEvent = createDomainEvent({
      eventId: "sqlite-stale-event-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 10,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Monitor", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    });

    assert.throws(
      () => eventStore.append(staleEvent, { expectedVersion: 0, fencingToken: 1 }),
      (err) => {
        assert.equal(err.code, "FENCING_TOKEN_STALE");
        assert.equal(err.providedToken, 1);
        assert.equal(err.currentToken, 2);
        return true;
      }
    );

    // Verify 0 events in database
    assert.equal(eventStore.getAll().length, 0);

    // Valid Worker 2 appends with token 2
    const validEvent = createDomainEvent({
      eventId: "sqlite-valid-event-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 10,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Monitor", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    });

    eventStore.append(validEvent, { expectedVersion: 0, fencingToken: 2 });
    assert.equal(eventStore.getAll().length, 1);
  } finally {
    db.close();
  }
});

test("partial commit protection: command with >=1 committed events CANNOT be taken over even if expired", () => {
  let currentTime = 1000;
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore({ commandStore, now: () => currentTime });
  const commandId = "partial-commit-expired-lease";

  // Worker 1 reserves at t=1000 (token 1, expires at 2000)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Desk", quantity: 1, amount: 600, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 1000,
    now: currentTime,
  });

  // Worker 1 commits 1 event before crashing / pausing
  const firstEvent = createDomainEvent({
    eventId: "event-partial-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 100,
    sequence: 1,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: "Desk", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });
  eventStore.append(firstEvent, { expectedVersion: 0, fencingToken: 1 });
  commandStore.recordEvent(commandId, firstEvent, { fencingToken: 1 });

  // Time advances past lease expiration to t=5000
  currentTime = 5000;

  // Worker 2 attempts takeover
  const takeover = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 2000,
    now: currentTime,
  });
  assert.equal(takeover.success, false);
  assert.equal(takeover.reason, "HAS_EVENTS");

  // Worker 2 engine checkout attempt reconciles and throws interrupted error (no re-execution)
  const engineWorker2 = new RollbackEngine({
    commandStore,
    eventStore,
    workerId: "worker-2",
    now: () => currentTime,
  });

  assert.throws(
    () =>
      engineWorker2.checkout(
        { item: "Desk", quantity: 1, amount: 600 },
        { commandId }
      ),
    (err) => {
      assert.equal(err.code, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");
      assert.equal(err.eventCommitted, true);
      assert.equal(err.retrySafe, false);
      assert.equal(err.retryAction, "MANUAL_RESOLUTION_REQUIRED");
      return true;
    }
  );
});
