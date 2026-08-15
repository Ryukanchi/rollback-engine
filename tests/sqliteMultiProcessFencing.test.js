const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { spawn } = require("node:child_process");

const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");
const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");

function runNodeProcess(scriptContent) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["-e", scriptContent], {
      cwd: process.cwd(),
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });

    child.on("error", reject);
  });
}

test("multi-process lease takeover and atomic zombie fencing in shared SQLite database", async () => {
  const dbPath = join(tmpdir(), `rollback-fencing-mp-${randomUUID()}.db`);
  const commandId = "mp-fenced-checkout-1";

  // Pre-initialize schema in database
  const initAdapters = createStorageAdapters({ type: "sqlite", dbPath });
  initAdapters.close();

  // Child Script A: Worker A reserves with 600ms lease TTL, pauses for 1400ms, then attempts stale append
  const workerAScript = `
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { createDomainEvent, EVENT_TYPES } = require('./src/domain/events');

    async function main() {
      const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
      const now = Date.now();
      const reservation = adapters.commandStore.reserve({
        commandId: ${JSON.stringify(commandId)},
        commandType: 'CHECKOUT',
        payload: { item: 'Drone', quantity: 1, amount: 999, simulateFailureAt: null },
        workerId: 'worker-process-a',
        leaseTtlMs: 600,
        now: now,
      });

      // Output reservation status to parent
      process.stdout.write(JSON.stringify({ step: 'RESERVED', record: reservation.record }) + '\\n');

      // Simulate network pause / GC stall / zombie state
      await new Promise((r) => setTimeout(r, 1400));

      // Worker A attempts to append event with stale token 1
      const staleEvent = createDomainEvent({
        eventId: 'zombie-event-drone-1',
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 50,
        sequence: 1,
        timestamp: new Date().toISOString(),
        payload: { item: 'Drone', quantity: 1 },
        metadata: {
          schemaVersion: 1,
          commandId: ${JSON.stringify(commandId)},
          correlationId: ${JSON.stringify(commandId)},
          causationId: ${JSON.stringify(commandId)},
        },
      });

      try {
        adapters.eventStore.append(staleEvent, { expectedVersion: 0, fencingToken: 1 });
        process.stdout.write(JSON.stringify({ step: 'ZOMBIE_APPEND_SUCCESS' }) + '\\n');
      } catch (appendErr) {
        process.stdout.write(JSON.stringify({
          step: 'ZOMBIE_APPEND_REJECTED',
          code: appendErr.code,
          providedToken: appendErr.providedToken,
          currentToken: appendErr.currentToken,
        }) + '\\n');
      } finally {
        adapters.close();
      }
    }

    main().catch((err) => {
      console.error(err);
      process.exit(1);
    });
  `;

  // Child Script B: Worker B waits 800ms (until Worker A's 600ms lease expires), takes over, and commits
  const workerBScript = `
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { RollbackEngine } = require('./src/application/rollbackEngine');

    async function main() {
      // Wait for Worker A's lease to expire
      await new Promise((r) => setTimeout(r, 800));

      const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
      const engine = new RollbackEngine({
        eventStore: adapters.eventStore,
        commandStore: adapters.commandStore,
        snapshotStore: adapters.snapshotStore,
        stateRepository: adapters.stateRepository,
        workerId: 'worker-process-b',
        leaseTtlMs: 3000,
      });

      try {
        const result = engine.checkout(
          { item: 'Drone', quantity: 1, amount: 999 },
          { commandId: ${JSON.stringify(commandId)} }
        );

        const cmd = adapters.commandStore.get(${JSON.stringify(commandId)});

        process.stdout.write(JSON.stringify({
          step: 'WORKER_B_SUCCESS',
          aggregateId: result.aggregateId,
          status: result.status,
          leaseToken: cmd.leaseToken,
        }) + '\\n');
      } catch (err) {
        process.stdout.write(JSON.stringify({
          step: 'WORKER_B_FAILED',
          code: err.code,
          message: err.message,
        }) + '\\n');
      } finally {
        adapters.close();
      }
    }

    main().catch((err) => {
      console.error(err);
      process.exit(1);
    });
  `;

  // Run both processes concurrently
  const [procAResult, procBResult] = await Promise.all([
    runNodeProcess(workerAScript),
    runNodeProcess(workerBScript),
  ]);

  assert.equal(procAResult.code, 0, `Worker A stderr: ${procAResult.stderr}`);
  assert.equal(procBResult.code, 0, `Worker B stderr: ${procBResult.stderr}`);

  const procALines = procAResult.stdout.trim().split("\n").filter(Boolean).map((l) => JSON.parse(l));
  const procBLines = procBResult.stdout.trim().split("\n").filter(Boolean).map((l) => JSON.parse(l));

  // Verify Worker A reserved with token 1
  const reservedStep = procALines.find((l) => l.step === "RESERVED");
  assert.notEqual(reservedStep, undefined);
  assert.equal(reservedStep.record.leaseToken, 1);
  assert.equal(reservedStep.record.leaseOwner, "worker-process-a");

  // Verify Worker B succeeded with token 2
  const workerBStep = procBLines.find((l) => l.step === "WORKER_B_SUCCESS");
  assert.notEqual(workerBStep, undefined);
  assert.equal(workerBStep.status, "completed");
  assert.equal(workerBStep.leaseToken, 2);

  // Verify Worker A zombie append was rejected with FENCING_TOKEN_STALE
  const zombieStep = procALines.find((l) => l.step === "ZOMBIE_APPEND_REJECTED");
  assert.notEqual(zombieStep, undefined);
  assert.equal(zombieStep.code, "FENCING_TOKEN_STALE");
  assert.equal(zombieStep.providedToken, 1);
  assert.equal(zombieStep.currentToken, 2);

  // Verify final authoritative state in SQLite database
  const verifyAdapters = createStorageAdapters({ type: "sqlite", dbPath });
  try {
    const allEvents = verifyAdapters.eventStore.getAll();
    assert.equal(allEvents.length, 3); // exactly the 3 events from Worker B
    assert.equal(allEvents.every((e) => e.eventId !== "zombie-event-drone-1"), true);

    const finalCmd = verifyAdapters.commandStore.get(commandId);
    assert.equal(finalCmd.status, "completed");
    assert.equal(finalCmd.leaseToken, 2);
    assert.equal(finalCmd.leaseOwner, null);

    const liveState = verifyAdapters.stateRepository.getByAggregateId(workerBStep.aggregateId);
    assert.equal(liveState.order.item, "Drone");
    assert.equal(liveState.lifecycle, "completed");
    assert.equal(liveState.payment.status, "charged");
  } finally {
    verifyAdapters.close();
  }
});
