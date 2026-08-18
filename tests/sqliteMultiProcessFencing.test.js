const test = require("node:test");
const assert = require("node:assert/strict");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { randomUUID } = require("node:crypto");
const { spawn } = require("node:child_process");
const readline = require("node:readline");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");

function createChildController(scriptContent) {
  const child = spawn(process.execPath, ["-e", scriptContent], {
    cwd: process.cwd(),
    stdio: ["pipe", "pipe", "pipe"],
  });

  const rl = readline.createInterface({ input: child.stdout });
  const lineQueue = [];
  const waiterQueue = [];

  rl.on("line", (line) => {
    if (!line.trim()) return;
    try {
      const parsed = JSON.parse(line.trim());
      if (waiterQueue.length > 0) {
        const resolve = waiterQueue.shift();
        resolve(parsed);
      } else {
        lineQueue.push(parsed);
      }
    } catch (err) {
      console.error("Failed to parse child JSON output:", line, err);
    }
  });

  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  let exitCode = null;
  const exitPromise = new Promise((resolve) => {
    child.on("close", (code) => {
      exitCode = code;
      resolve({ code, stderr });
    });
  });

  function nextMessage() {
    if (lineQueue.length > 0) {
      return Promise.resolve(lineQueue.shift());
    }
    return new Promise((resolve) => {
      waiterQueue.push(resolve);
    });
  }

  function sendCommand(cmdString) {
    child.stdin.write(`${cmdString}\n`);
  }

  return {
    child,
    nextMessage,
    sendCommand,
    waitExit: () => exitPromise,
    getStderr: () => stderr,
  };
}

test("multi-process lease takeover and atomic zombie fencing while command is still processing", async () => {
  const dbPath = join(tmpdir(), `rollback-fencing-barrier-${randomUUID()}.db`);
  const commandId = "mp-fenced-checkout-barrier-1";

  // Pre-initialize schema in database
  const initAdapters = createStorageAdapters({ type: "sqlite", dbPath });
  initAdapters.close();

  // Child Script A: Worker A reserves with 500ms lease TTL and waits for signal to attempt stale append
  const workerAScript = `
    const readline = require('node:readline');
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { createDomainEvent, EVENT_TYPES } = require('./src/domain/events');

    async function main() {
      const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
      const reservation = adapters.commandStore.reserve({
        commandId: ${JSON.stringify(commandId)},
        commandType: 'CHECKOUT',
        payload: { item: 'Drone', quantity: 1, amount: 999, simulateFailureAt: null },
        workerId: 'worker-process-a',
        leaseTtlMs: 500,
      });

      // Signal parent that reservation is complete
      process.stdout.write(JSON.stringify({ step: 'RESERVED', record: reservation.record }) + '\\n');

      const rl = readline.createInterface({ input: process.stdin });
      for await (const line of rl) {
        if (line.trim() === 'ATTEMPT_STALE_APPEND') {
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
            process.exit(0);
          }
        }
      }
    }

    main().catch((err) => {
      console.error(err);
      process.exit(1);
    });
  `;

  // Child Script B: Worker B waits for TAKE_OVER signal, takes over (status remains processing), then appends valid event
  const workerBScript = `
    const readline = require('node:readline');
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { createDomainEvent, EVENT_TYPES } = require('./src/domain/events');

    async function main() {
      const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
      const rl = readline.createInterface({ input: process.stdin });

      for await (const line of rl) {
        if (line.trim() === 'TAKE_OVER') {
          const takeover = adapters.commandStore.takeOverExpired({
            commandId: ${JSON.stringify(commandId)},
            workerId: 'worker-process-b',
            leaseTtlMs: 5000,
          });

          process.stdout.write(JSON.stringify({
            step: 'TAKEOVER_COMPLETE_TOKEN_2',
            success: takeover.success,
            leaseToken: takeover.record?.leaseToken,
            status: takeover.record?.status,
            leaseOwner: takeover.record?.leaseOwner,
          }) + '\\n');
        } else if (line.trim() === 'COMMIT_VALID_EVENT') {
          const validEvent = createDomainEvent({
            eventId: 'valid-event-drone-1',
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
            const appended = adapters.eventStore.append(validEvent, { expectedVersion: 0, fencingToken: 2 });
            process.stdout.write(JSON.stringify({
              step: 'VALID_APPEND_SUCCESS',
              eventId: appended.eventId,
            }) + '\\n');
          } catch (err) {
            process.stdout.write(JSON.stringify({
              step: 'VALID_APPEND_FAILED',
              code: err.code,
              message: err.message,
            }) + '\\n');
          } finally {
            adapters.close();
            process.exit(0);
          }
        }
      }
    }

    main().catch((err) => {
      console.error(err);
      process.exit(1);
    });
  `;

  const procA = createChildController(workerAScript);
  const procB = createChildController(workerBScript);

  try {
    // 1. Worker A reserves command with token 1 and owner 'worker-process-a'
    const msgA1 = await procA.nextMessage();
    assert.equal(msgA1.step, "RESERVED");
    assert.equal(msgA1.record.leaseToken, 1);
    assert.equal(msgA1.record.leaseOwner, "worker-process-a");
    assert.equal(msgA1.record.status, "processing");

    // 2. Wait 600ms so Worker A's 500ms lease is guaranteed physically expired
    await new Promise((r) => setTimeout(r, 600));

    // 3. Trigger Worker B to take over expired lease
    procB.sendCommand("TAKE_OVER");
    const msgB1 = await procB.nextMessage();
    assert.equal(msgB1.step, "TAKEOVER_COMPLETE_TOKEN_2");
    assert.equal(msgB1.success, true);
    assert.equal(msgB1.leaseToken, 2);
    assert.equal(msgB1.status, "processing");
    assert.equal(msgB1.leaseOwner, "worker-process-b");

    // 4. Verify intermediate database state: command is processing, token is 2, events = 0
    const midAdapters = createStorageAdapters({ type: "sqlite", dbPath });
    try {
      const cmdRow = midAdapters.commandStore.get(commandId);
      assert.equal(cmdRow.status, "processing");
      assert.equal(cmdRow.leaseToken, 2);
      assert.equal(cmdRow.leaseOwner, "worker-process-b");
      assert.equal(midAdapters.eventStore.getAll().length, 0);
    } finally {
      midAdapters.close();
    }

    // 5. Trigger Worker A (Zombie) to attempt append with stale token 1 while command is STILL PROCESSING
    procA.sendCommand("ATTEMPT_STALE_APPEND");
    const msgA2 = await procA.nextMessage();
    assert.equal(msgA2.step, "ZOMBIE_APPEND_REJECTED");
    assert.equal(msgA2.code, "FENCING_TOKEN_STALE");
    assert.equal(msgA2.providedToken, 1);
    assert.equal(msgA2.currentToken, 2);

    // 6. Verify database after stale append: command is STILL processing, events is STILL 0
    const postStaleAdapters = createStorageAdapters({ type: "sqlite", dbPath });
    try {
      const cmdRow = postStaleAdapters.commandStore.get(commandId);
      assert.equal(cmdRow.status, "processing");
      assert.equal(cmdRow.leaseToken, 2);
      assert.equal(cmdRow.leaseOwner, "worker-process-b");
      assert.equal(postStaleAdapters.eventStore.getAll().length, 0);
    } finally {
      postStaleAdapters.close();
    }

    // 7. Trigger Worker B (Current Owner) to append valid event with current token 2
    procB.sendCommand("COMMIT_VALID_EVENT");
    const msgB2 = await procB.nextMessage();
    assert.equal(msgB2.step, "VALID_APPEND_SUCCESS");
    assert.equal(msgB2.eventId, "valid-event-drone-1");

    // 8. Verify final database state: exactly 1 event committed by Worker B, 0 from Worker A
    const finalAdapters = createStorageAdapters({ type: "sqlite", dbPath });
    try {
      const allEvents = finalAdapters.eventStore.getAll();
      assert.equal(allEvents.length, 1);
      assert.equal(allEvents[0].eventId, "valid-event-drone-1");
      assert.equal(allEvents[0].sequence, 1);

      const finalCmd = finalAdapters.commandStore.get(commandId);
      assert.equal(finalCmd.status, "processing");
      assert.equal(finalCmd.leaseToken, 2);
    } finally {
      finalAdapters.close();
    }

    await procA.waitExit();
    await procB.waitExit();
  } catch (err) {
    procA.child.kill();
    procB.child.kill();
    throw err;
  }
});

test("multi-process partial commit: unrecorded event in events table blocks takeover after lease expiry", async () => {
  const dbPath = join(tmpdir(), `rollback-partial-mp-${randomUUID()}.db`);
  const commandId = "mp-partial-commit-cmd-1";

  // Pre-initialize schema in database
  const initAdapters = createStorageAdapters({ type: "sqlite", dbPath });
  initAdapters.close();

  // Child Script A: Worker A reserves with 500ms lease, appends event to EventStore, but crashes BEFORE commandStore.recordEvent()
  const workerAScript = `
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');
    const { createDomainEvent, EVENT_TYPES } = require('./src/domain/events');

    async function main() {
      const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
      const reservation = adapters.commandStore.reserve({
        commandId: ${JSON.stringify(commandId)},
        commandType: 'CHECKOUT',
        payload: { item: 'Microscope', quantity: 1, amount: 750, simulateFailureAt: null },
        workerId: 'worker-process-a',
        leaseTtlMs: 500,
      });

      const firstEvent = createDomainEvent({
        eventId: 'event-unrecorded-partial-1',
        eventType: EVENT_TYPES.ORDER_CREATED,
        aggregateId: 77,
        sequence: 1,
        timestamp: new Date().toISOString(),
        payload: { item: 'Microscope', quantity: 1 },
        metadata: {
          schemaVersion: 1,
          commandId: ${JSON.stringify(commandId)},
          correlationId: ${JSON.stringify(commandId)},
          causationId: ${JSON.stringify(commandId)},
        },
      });

      // Authoritatively commit event
      adapters.eventStore.append(firstEvent, { expectedVersion: 0, fencingToken: 1 });

      // INTENTIONALLY SKIP commandStore.recordEvent() to simulate crash immediately after EventStore append!
      process.stdout.write(JSON.stringify({ step: 'COMMITTED_WITHOUT_RECORD_EVENT' }) + '\\n');
      adapters.close();
      process.exit(0);
    }

    main().catch((err) => {
      console.error(err);
      process.exit(1);
    });
  `;

  // Child Script B: Worker B attempts takeover after lease expiry
  const workerBScript = `
    const { createStorageAdapters } = require('./src/infrastructure/storageFactory');

    async function main() {
      const adapters = createStorageAdapters({ type: 'sqlite', dbPath: ${JSON.stringify(dbPath)} });
      const takeover = adapters.commandStore.takeOverExpired({
        commandId: ${JSON.stringify(commandId)},
        workerId: 'worker-process-b',
        leaseTtlMs: 3000,
      });

      process.stdout.write(JSON.stringify({
        step: 'TAKEOVER_ATTEMPT',
        success: takeover.success,
        reason: takeover.reason,
      }) + '\\n');

      adapters.close();
      process.exit(0);
    }

    main().catch((err) => {
      console.error(err);
      process.exit(1);
    });
  `;

  // 1. Run Worker A
  const procA = createChildController(workerAScript);
  const msgA = await procA.nextMessage();
  assert.equal(msgA.step, "COMMITTED_WITHOUT_RECORD_EVENT");
  await procA.waitExit();

  // 2. Wait 600ms for Worker A's 500ms lease to expire
  await new Promise((r) => setTimeout(r, 600));

  // 3. Run Worker B to attempt takeover
  const procB = createChildController(workerBScript);
  const msgB = await procB.nextMessage();
  assert.equal(msgB.step, "TAKEOVER_ATTEMPT");
  assert.equal(msgB.success, false);
  assert.equal(msgB.reason, "HAS_EVENTS");
  await procB.waitExit();

  // 4. Verify database: event exists in events table, command.event_range is NULL, token is still 1
  const verifyAdapters = createStorageAdapters({ type: "sqlite", dbPath });
  try {
    const events = verifyAdapters.eventStore.getAll();
    assert.equal(events.length, 1);
    assert.equal(events[0].eventId, "event-unrecorded-partial-1");

    const cmd = verifyAdapters.commandStore.get(commandId);
    assert.equal(cmd.status, "processing");
    assert.equal(cmd.leaseToken, 1);
    assert.equal(cmd.leaseOwner, "worker-process-a");
    assert.equal(cmd.eventRange, null);
  } finally {
    verifyAdapters.close();
  }
});
