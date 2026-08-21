const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { DatabaseSync } = require("node:sqlite");

const { RollbackEngine } = require("../src/application/rollbackEngine");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { InMemoryCommandStore } = require("../src/infrastructure/inMemoryCommandStore");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");
const { SqliteCommandStore } = require("../src/infrastructure/sqlite/sqliteCommandStore");
const { SqliteEventStore } = require("../src/infrastructure/sqlite/sqliteEventStore");
const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const {
  commandReceiptMetadata,
} = require("./support/commandReceiptFixtures");

function createEngineWithCustomClock({
  storageType = "memory",
  dbPath = ":memory:",
  workerId = "worker-1",
  leaseTtlMs = 1000,
  getNow,
  diagnostics = [],
} = {}) {
  const adapters = createStorageAdapters({
    type: storageType,
    dbPath,
    leaseNow: getNow,
  });
  const engine = new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    workerId,
    leaseTtlMs,
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
  const commandStore = new InMemoryCommandStore({ now: () => currentTime });
  const eventStore = new InMemoryEventStore({ commandStore });
  const commandId = "in-flight-command";

  // Worker 1 reserves command with lease expiring at 3000
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Bike", quantity: 1, amount: 500, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 2000,
  });

  // Worker 2 attempts execution at currentTime = 2000 (lease still valid until 3000)
  currentTime = 2000;
  const engineWorker2 = new RollbackEngine({
    commandStore,
    eventStore,
    workerId: "worker-2",
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
  const commandStore = new InMemoryCommandStore({ now: () => currentTime });
  const eventStore = new InMemoryEventStore({ commandStore });
  const commandId = "abandoned-processing-command";
  const diagnostics = [];

  // Worker 1 reserves at t=1000 with 2000ms TTL (expires at 3000)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Phone", quantity: 1, amount: 800, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 2000,
  });

  // Time advances to t=3500 (lease is expired, 0 events committed)
  currentTime = 3500;
  const engineWorker2 = new RollbackEngine({
    commandStore,
    eventStore,
    workerId: "worker-2",
    leaseTtlMs: 3000,
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
  const commandStore = new InMemoryCommandStore({ now: () => currentTime });
  const eventStore = new InMemoryEventStore({ commandStore });
  const commandId = "zombie-test-command";

  // Worker 1 reserves at t=1000 (token 1, expires at 2000)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Tablet", quantity: 1, amount: 400, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 1000,
  });

  // Worker 2 takes over at t=2500 (token becomes 2, expires at 4500)
  currentTime = 2500;
  const takeover = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 2000,
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
    let currentTime = 1000;
    const commandStore = new SqliteCommandStore({ db, now: () => currentTime });
    const eventStore = new SqliteEventStore({ db });
    const commandId = "sqlite-fencing-cmd";

    // Worker 1 reserves command with token 1
    commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: { item: "Monitor", quantity: 1, amount: 300 },
      workerId: "worker-1",
      leaseTtlMs: 1000,
    });

    // Worker 2 takes over after expiry at t=2500 -> token becomes 2
    currentTime = 2500;
    const takeover = commandStore.takeOverExpired({
      commandId,
      workerId: "worker-2",
      leaseTtlMs: 2000,
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
  const commandStore = new InMemoryCommandStore({ now: () => currentTime });
  const eventStore = new InMemoryEventStore({ commandStore });
  commandStore.setEventStore(eventStore);
  const commandId = "partial-commit-expired-lease";

  // Worker 1 reserves at t=1000 (token 1, expires at 2000)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Desk", quantity: 1, amount: 600, simulateFailureAt: null },
    workerId: "worker-1",
    leaseTtlMs: 1000,
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
  });
  assert.equal(takeover.success, false);
  assert.equal(takeover.reason, "HAS_EVENTS");

  // Worker 2 engine checkout attempt reconciles and throws interrupted error (no re-execution)
  const engineWorker2 = new RollbackEngine({
    commandStore,
    eventStore,
    workerId: "worker-2",
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

test("authoritative event in events table + stale/empty command event_range blocks takeover", () => {
  const dbPath = join(tmpdir(), `rollback-authoritative-takeover-${randomUUID()}.db`);
  const db = createSqliteDatabase({ path: dbPath });

  try {
    let currentTime = 1000;
    const commandStore = new SqliteCommandStore({ db, now: () => currentTime });
    const eventStore = new SqliteEventStore({ db });
    const commandId = "unrecorded-event-cmd";

    // Worker 1 reserves at t=1000 (expires at 2000)
    commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: { item: "Camera", quantity: 1, amount: 500 },
      workerId: "worker-1",
      leaseTtlMs: 1000,
    });

    // Worker 1 commits event directly to eventStore at t=1500, but crashes BEFORE commandStore.recordEvent()
    currentTime = 1500;
    const committedEvent = createDomainEvent({
      eventId: "camera-evt-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 44,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Camera", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    });
    eventStore.append(committedEvent, { expectedVersion: 0, fencingToken: 1 });

    // Verify command store record still has event_range == null
    const cmdBefore = commandStore.get(commandId);
    assert.equal(cmdBefore.eventRange, null);
    assert.equal(cmdBefore.leaseToken, 1);

    // At t=3000 (lease expired), Worker 2 attempts takeover
    currentTime = 3000;
    const takeover = commandStore.takeOverExpired({
      commandId,
      workerId: "worker-2",
      leaseTtlMs: 2000,
    });

    // Takeover MUST be rejected because authoritative events table contains an event
    assert.equal(takeover.success, false);
    assert.equal(takeover.reason, "HAS_EVENTS");

    // Command ownership and token must remain unchanged
    const cmdAfter = commandStore.get(commandId);
    assert.equal(cmdAfter.status, "processing");
    assert.equal(cmdAfter.leaseToken, 1);
    assert.equal(cmdAfter.leaseOwner, "worker-1");
  } finally {
    db.close();
  }
});

test("missing fencing token cannot append for leased command (FENCING_TOKEN_REQUIRED)", () => {
  // Test SQLite store
  const dbPath = join(tmpdir(), `rollback-missing-token-${randomUUID()}.db`);
  const db = createSqliteDatabase({ path: dbPath });

  try {
    const commandStore = new SqliteCommandStore({ db, now: () => 1000 });
    const eventStore = new SqliteEventStore({ db });
    const commandId = "leased-no-token-cmd";

    commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: { item: "Speaker", quantity: 1, amount: 200 },
      workerId: "worker-1",
      leaseTtlMs: 5000,
    });

    const event = createDomainEvent({
      eventId: "speaker-evt-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 90,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Speaker", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    });

    // Append without fencingToken option
    assert.throws(
      () => eventStore.append(event, { expectedVersion: 0 }),
      (err) => {
        assert.equal(err.code, "FENCING_TOKEN_REQUIRED");
        assert.equal(err.commandId, commandId);
        assert.equal(err.eventCommitted, false);
        assert.equal(err.retrySafe, false);
        return true;
      }
    );

    // Append with fencingToken: undefined explicitly
    assert.throws(
      () => eventStore.append(event, { expectedVersion: 0, fencingToken: undefined }),
      (err) => {
        assert.equal(err.code, "FENCING_TOKEN_REQUIRED");
        return true;
      }
    );

    // Verify 0 events in store
    assert.equal(eventStore.getAll().length, 0);
  } finally {
    db.close();
  }

  // Test In-Memory store parity
  const memCommandStore = new InMemoryCommandStore({ now: () => 1000 });
  const memEventStore = new InMemoryEventStore({ commandStore: memCommandStore });
  memCommandStore.reserve({
    commandId: "mem-no-token-cmd",
    commandType: "CHECKOUT",
    payload: { item: "Speaker", quantity: 1, amount: 200 },
    workerId: "worker-1",
    leaseTtlMs: 5000,
  });

  const memEvent = createDomainEvent({
    eventId: "mem-speaker-evt-1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 91,
    sequence: 1,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: "Speaker", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId: "mem-no-token-cmd",
      correlationId: "mem-no-token-cmd",
      causationId: "mem-no-token-cmd",
    },
  });

  assert.throws(
    () => memEventStore.append(memEvent, { expectedVersion: 0 }),
    (err) => {
      assert.equal(err.code, "FENCING_TOKEN_REQUIRED");
      assert.equal(err.eventCommitted, false);
      return true;
    }
  );
  assert.equal(memEventStore.getAll().length, 0);
});

test("stale token rejected while command is STILL processing under new owner", () => {
  const dbPath = join(tmpdir(), `rollback-still-processing-${randomUUID()}.db`);
  const db = createSqliteDatabase({ path: dbPath });

  try {
    let currentTime = 1000;
    const commandStore = new SqliteCommandStore({ db, now: () => currentTime });
    const eventStore = new SqliteEventStore({ db });
    const commandId = "still-proc-cmd";

    // Worker 1 reserves at t=1000 (token 1, expires at 2000)
    commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: { item: "Headphones", quantity: 1, amount: 150 },
      workerId: "worker-1",
      leaseTtlMs: 1000,
    });

    // Worker 2 takes over at t=2500 -> token becomes 2, status remains PROCESSING
    currentTime = 2500;
    const takeover = commandStore.takeOverExpired({
      commandId,
      workerId: "worker-2",
      leaseTtlMs: 3000,
    });
    assert.equal(takeover.success, true);
    assert.equal(takeover.record.leaseToken, 2);
    assert.equal(takeover.record.status, "processing");

    // Worker 1 attempts append with stale token 1 while status is STILL processing
    const staleEvent = createDomainEvent({
      eventId: "hp-stale-evt-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 33,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Headphones", quantity: 1 },
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

    // Verify database state: status is STILL processing, events = 0
    const cmdMid = commandStore.get(commandId);
    assert.equal(cmdMid.status, "processing");
    assert.equal(cmdMid.leaseToken, 2);
    assert.equal(eventStore.getAll().length, 0);

    // Worker 2 appends with valid current token 2
    const validEvent = createDomainEvent({
      eventId: "hp-valid-evt-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 33,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Headphones", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId,
        correlationId: commandId,
        causationId: commandId,
      },
    });

    const appended = eventStore.append(validEvent, { expectedVersion: 0, fencingToken: 2 });
    assert.equal(appended.eventId, "hp-valid-evt-1");
    assert.equal(eventStore.getAll().length, 1);
  } finally {
    db.close();
  }
});

test("separation of protection layers: status check vs token check tested separately", () => {
  const dbPath = join(tmpdir(), `rollback-layer-sep-${randomUUID()}.db`);
  const db = createSqliteDatabase({ path: dbPath });

  try {
    let currentTime = 1000;
    const commandStore = new SqliteCommandStore({ db, now: () => currentTime });
    const eventStore = new SqliteEventStore({ db });
    const cmdProcessing = "cmd-layer-processing";
    const cmdCompleted = "cmd-layer-completed";

    // Layer 1: Token mismatch while status is 'processing'
    commandStore.reserve({
      commandId: cmdProcessing,
      commandType: "CHECKOUT",
      payload: { item: "Item1", quantity: 1, amount: 100 },
      workerId: "worker-1",
      leaseTtlMs: 1000,
    });
    currentTime = 2500;
    commandStore.takeOverExpired({
      commandId: cmdProcessing,
      workerId: "worker-2",
      leaseTtlMs: 2000,
    });

    const evtForProcessing = createDomainEvent({
      eventId: "evt-proc-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 101,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Item1", quantity: 1 },
      metadata: { schemaVersion: 1, commandId: cmdProcessing, correlationId: cmdProcessing, causationId: cmdProcessing },
    });

    // Rejection is strictly due to token mismatch (1 !== 2), not status
    assert.throws(
      () => eventStore.append(evtForProcessing, { expectedVersion: 0, fencingToken: 1 }),
      (err) => {
        assert.equal(err.code, "FENCING_TOKEN_STALE");
        assert.equal(err.providedToken, 1);
        assert.equal(err.currentToken, 2);
        return true;
      }
    );

    // Layer 2: Status check rejection when status is 'completed' (even if presenting historical token 1)
    currentTime = 1000;
    commandStore.reserve({
      commandId: cmdCompleted,
      commandType: "CHECKOUT",
      payload: { item: "Item2", quantity: 1, amount: 100 },
      workerId: "worker-1",
      leaseTtlMs: 5000,
    });
    commandStore.complete(
      cmdCompleted,
      { aggregateId: 102, status: "completed" },
      { fencingToken: 1, receiptMetadata: commandReceiptMetadata() }
    );

    const evtForCompleted = createDomainEvent({
      eventId: "evt-comp-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId: 102,
      sequence: 1,
      timestamp: "2026-08-15T12:00:00.000Z",
      payload: { item: "Item2", quantity: 1 },
      metadata: { schemaVersion: 1, commandId: cmdCompleted, correlationId: cmdCompleted, causationId: cmdCompleted },
    });

    // Rejection is due to status === 'completed', refusing any mutation
    assert.throws(
      () => eventStore.append(evtForCompleted, { expectedVersion: 0, fencingToken: 1 }),
      (err) => {
        assert.equal(err.code, "FENCING_TOKEN_STALE");
        return true;
      }
    );
  } finally {
    db.close();
  }
});

test("migrated legacy SQLite partial commit cannot be taken over", () => {
  const dbPath = join(tmpdir(), `rollback-migrated-partial-${randomUUID()}.db`);
  const rawDb = new DatabaseSync(dbPath);

  // Manually create legacy v1 schema
  rawDb.exec(`
    CREATE TABLE events (
      event_id TEXT PRIMARY KEY NOT NULL,
      aggregate_id ANY NOT NULL,
      sequence INTEGER NOT NULL,
      command_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      timestamp TEXT NOT NULL,
      payload TEXT NOT NULL,
      metadata TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE (aggregate_id, sequence)
    );
    CREATE TABLE commands (
      command_id TEXT PRIMARY KEY NOT NULL,
      command_type TEXT NOT NULL,
      payload TEXT NOT NULL,
      status TEXT NOT NULL,
      aggregate_id ANY,
      event_range TEXT,
      result TEXT,
      error TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    PRAGMA user_version = 1;
  `);

  const legacyCmdId = "legacy-partial-cmd-1";
  rawDb.prepare(`
    INSERT INTO commands (command_id, command_type, payload, status)
    VALUES (?, 'CHECKOUT', '{"item":"LegacyItem","quantity":1,"amount":100}', 'processing')
  `).run(legacyCmdId);

  rawDb.prepare(`
    INSERT INTO events (event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata)
    VALUES ('legacy-evt-1', 501, 1, ?, 'ORDER_CREATED', '2026-08-15T12:00:00.000Z', '{"item":"LegacyItem"}', '{"commandId":"${legacyCmdId}"}')
  `).run(legacyCmdId);

  rawDb.close();

  // Open with storage adapters -> triggers v1 to v2 migration
  const adapters = createStorageAdapters({
    type: "sqlite",
    dbPath,
    leaseNow: () => Date.now() + 10000,
  });
  try {
    const takeover = adapters.commandStore.takeOverExpired({
      commandId: legacyCmdId,
      workerId: "worker-new",
      leaseTtlMs: 2000,
    });

    // Authoritative event in events table prevents takeover even on migrated legacy databases
    assert.equal(takeover.success, false);
    assert.equal(takeover.reason, "HAS_EVENTS");

    const cmd = adapters.commandStore.get(legacyCmdId);
    assert.equal(cmd.status, "processing");
    assert.equal(cmd.leaseToken, 1);
  } finally {
    adapters.close();
  }
});
