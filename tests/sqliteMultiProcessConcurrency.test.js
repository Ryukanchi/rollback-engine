const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { spawn } = require("node:child_process");

const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");
const { SqliteEventStore } = require("../src/infrastructure/sqlite/sqliteEventStore");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");

function runWorker({ dbPath, workerId, aggregateId, sequence, expectedVersion, delayMs = 0 }) {
  return new Promise((resolve) => {
    const workerScript = `
      const { createSqliteDatabase } = require('./src/infrastructure/sqlite/sqliteDatabase');
      const { SqliteEventStore } = require('./src/infrastructure/sqlite/sqliteEventStore');
      const { createDomainEvent, EVENT_TYPES } = require('./src/domain/events');

      const db = createSqliteDatabase({ path: ${JSON.stringify(dbPath)}, busyTimeout: 5000 });
      const eventStore = new SqliteEventStore({ db });

      const event = createDomainEvent({
        eventId: 'evt-worker-' + ${JSON.stringify(workerId)} + '-' + Date.now(),
        eventType: EVENT_TYPES.INVENTORY_RESERVED,
        aggregateId: ${aggregateId},
        sequence: ${sequence},
        payload: { reservationId: ${JSON.stringify("res-" + workerId)}, item: 'Keyboard', quantity: 1 },
        metadata: {
          schemaVersion: 1,
          commandId: 'cmd-worker-' + ${JSON.stringify(workerId)},
          correlationId: 'corr-' + ${JSON.stringify(workerId)},
          causationId: 'cause-initial',
        },
      });

      setTimeout(() => {
        try {
          const stored = eventStore.append(event, { expectedVersion: ${expectedVersion} });
          db.close();
          process.stdout.write(JSON.stringify({ status: 'SUCCESS', eventId: stored.eventId }));
        } catch (err) {
          db.close();
          process.stdout.write(JSON.stringify({
            status: 'ERROR',
            code: err.code,
            message: err.message,
            actualVersion: err.actualVersion,
            expectedVersion: err.expectedVersion,
          }));
        }
      }, ${delayMs});
    `;

    const child = spawn(process.execPath, ["-e", workerScript], {
      cwd: process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    child.on("close", (exitCode) => {
      let parsed = null;
      try {
        parsed = JSON.parse(stdout.trim());
      } catch {
        parsed = { status: "PARSE_ERROR", stdout, stderr, exitCode };
      }
      resolve(parsed);
    });
  });
}

test("multi-process optimistic concurrency: exactly one writer succeeds and one detects version conflict", async () => {
  const dbPath = join(tmpdir(), `rollback-concurrency-${randomUUID()}.db`);
  const aggregateId = 100;

  // Initialize DB with event sequence 1 (expectedVersion 0)
  const initialDb = createSqliteDatabase({ path: dbPath, busyTimeout: 5000 });
  const initialStore = new SqliteEventStore({ db: initialDb });

  initialStore.append(
    createDomainEvent({
      eventId: "evt-init-1",
      eventType: EVENT_TYPES.ORDER_CREATED,
      aggregateId,
      sequence: 1,
      payload: { item: "Keyboard", quantity: 1 },
      metadata: {
        schemaVersion: 1,
        commandId: "cmd-init-1",
        correlationId: "corr-init",
        causationId: "cause-init",
      },
    }),
    { expectedVersion: 0 }
  );

  initialDb.close();

  // Launch two concurrent Node.js processes targeting the same SQLite DB file
  // Both processes try to append sequence 2 with expectedVersion = 1
  const [worker1Result, worker2Result] = await Promise.all([
    runWorker({ dbPath, workerId: "A", aggregateId, sequence: 2, expectedVersion: 1, delayMs: 5 }),
    runWorker({ dbPath, workerId: "B", aggregateId, sequence: 2, expectedVersion: 1, delayMs: 5 }),
  ]);

  const results = [worker1Result, worker2Result];
  const successes = results.filter((r) => r.status === "SUCCESS");
  const conflicts = results.filter((r) => r.status === "ERROR" && r.code === "OPTIMISTIC_CONCURRENCY_CONFLICT");

  // Invariant 1: Exactly one process succeeds and one experiences optimistic concurrency conflict
  assert.equal(successes.length, 1, `Expected 1 success, got: ${JSON.stringify(results)}`);
  assert.equal(conflicts.length, 1, `Expected 1 conflict, got: ${JSON.stringify(results)}`);

  // Invariant 2: The conflict is an OPTIMISTIC_CONCURRENCY_CONFLICT (not a SQLITE_BUSY or lock error)
  assert.equal(conflicts[0].expectedVersion, 1);
  assert.equal(conflicts[0].actualVersion, 2);

  // Invariant 3: Event stream on disk is contiguous, valid and contains exactly 2 events
  const verifyDb = createSqliteDatabase({ path: dbPath });
  const verifyStore = new SqliteEventStore({ db: verifyDb });

  try {
    const allEvents = verifyStore.getByAggregateId(aggregateId);
    assert.equal(allEvents.length, 2);
    assert.equal(allEvents[0].sequence, 1);
    assert.equal(allEvents[1].sequence, 2);
    assert.equal(verifyStore.getLastSequence(aggregateId), 2);
  } finally {
    verifyDb.close();
  }
});
