const test = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");

const { InMemoryCommandStore } = require("../src/infrastructure/inMemoryCommandStore");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const { InMemorySnapshotStore } = require("../src/infrastructure/inMemorySnapshotStore");
const { InMemoryStateRepository } = require("../src/infrastructure/inMemoryStateRepository");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { COMMAND_STATUSES } = require("../src/application/storeContracts");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");

// --- Helpers ---

function createTestEvent({ commandId, aggregateId = 1, sequence = 1, eventId } = {}) {
  return createDomainEvent({
    eventId: eventId || `evt-${randomUUID()}`,
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId,
    sequence,
    timestamp: new Date().toISOString(),
    payload: { item: "Widget", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId: commandId || `cmd-${randomUUID()}`,
      correlationId: randomUUID(),
      causationId: randomUUID(),
    },
  });
}

function createLeaseStores({ now } = {}) {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore({ now });
  commandStore.setEventStore(eventStore);
  eventStore.setCommandStore(commandStore);
  return { commandStore, eventStore };
}

function createEngineWithClock({
  workerId = "worker-1",
  leaseTtlMs = 1000,
  getNow,
} = {}) {
  const commandStore = new InMemoryCommandStore();
  const eventStore = new InMemoryEventStore({ now: getNow });
  const snapshotStore = new InMemorySnapshotStore();
  const stateRepository = new InMemoryStateRepository();
  commandStore.setEventStore(eventStore);
  eventStore.setCommandStore(commandStore);

  const engine = new RollbackEngine({
    eventStore,
    commandStore,
    snapshotStore,
    stateRepository,
    workerId,
    leaseTtlMs,
    now: getNow,
  });

  return { engine, commandStore, eventStore };
}

// === Scenario A: stale token cannot release current owner's command ===

test("Z-1: stale token cannot release current owner's command after takeover", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-a";

  // Worker 1 reserves (token=1)
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 100,
    now: 1000,
  });

  // Expire and takeover by worker 2 (token=2)
  const takeover = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: 2000,
  });
  assert.equal(takeover.success, true);
  assert.equal(takeover.record.leaseToken, 2);

  // Worker 1 tries to release with stale token=1
  assert.throws(
    () => commandStore.release(commandId, { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );

  // Command must still be owned by worker-2
  const record = commandStore.get(commandId);
  assert.equal(record.status, COMMAND_STATUSES.PROCESSING);
  assert.equal(record.leaseOwner, "worker-2");
  assert.equal(record.leaseToken, 2);
});

// === Scenario B: stale token cannot releaseFailed current owner's command ===

test("Z-1: stale token cannot releaseFailed current owner's command", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-b";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 100,
    now: 1000,
  });

  // Fail the command
  commandStore.fail(commandId, { code: "SOME_ERROR" }, { fencingToken: 1 });

  // releaseFailed with wrong token
  assert.throws(
    () => commandStore.releaseFailed(commandId, "SOME_ERROR", { fencingToken: 99 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
});

// === Scenario C: FENCING_TOKEN_REQUIRED/FENCING_CONTEXT_INVALID never triggers unsafe release ===

// Each of these codes means "this worker has lost the right to act". The
// coordinator must re-throw them untouched: releasing or failing the
// reservation would let the fenced worker finalize a row it no longer owns.
for (const fencingCode of [
  "FENCING_TOKEN_REQUIRED",
  "FENCING_CONTEXT_INVALID",
  "FENCING_TOKEN_STALE",
  "COMMAND_LEASE_EXPIRED",
]) {
  test(`Z-2: ${fencingCode} from the event store leaves the reservation untouched`, () => {
    let currentTime = 1000;
    const commandStore = new InMemoryCommandStore();
    const eventStore = new InMemoryEventStore({ now: () => currentTime });
    commandStore.setEventStore(eventStore);
    eventStore.setCommandStore(commandStore);

    // The append is rejected by the fencing layer before anything is committed.
    eventStore.append = () => {
      throw Object.assign(new Error(`${fencingCode} raised by the event store`), {
        code: fencingCode,
        eventCommitted: false,
      });
    };

    const engine = new RollbackEngine({
      eventStore,
      commandStore,
      snapshotStore: new InMemorySnapshotStore(),
      stateRepository: new InMemoryStateRepository(),
      workerId: "worker-1",
      leaseTtlMs: 5000,
      now: () => currentTime,
    });

    const commandId = `fencing-nocleanup-${fencingCode}`;

    assert.throws(
      () => engine.checkout({ item: "Laptop", quantity: 1, amount: 500 }, { commandId }),
      (error) => {
        assert.equal(error.code, fencingCode);
        return true;
      }
    );

    // The reservation must survive exactly as it was: not released, not failed,
    // and still owned by the generation that holds it.
    const stored = commandStore.get(commandId);
    assert.equal(stored.status, COMMAND_STATUSES.PROCESSING);
    assert.equal(stored.leaseOwner, "worker-1");
    assert.equal(stored.leaseToken, 1);
    assert.equal(stored.eventRange, null);
    assert.equal(stored.error, null);
    assert.equal(eventStore.getAll().length, 0);
  });
}

// === Scenario D: releasing command preserves fencing (no ABA) ===

test("Z-4: release transitions to 'released' status preserving the fencing token", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-d";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 5000,
    now: 1000,
  });

  commandStore.release(commandId, { fencingToken: 1 });

  const released = commandStore.get(commandId);
  assert.equal(released.status, COMMAND_STATUSES.RELEASED);
  assert.equal(released.leaseToken, 1); // token preserved
  assert.equal(released.leaseOwner, null);
  assert.equal(released.leaseExpiresAt, null);
});

// === Scenario E: old fencing generation cannot become valid after re-reservation (ABA prevention) ===

test("Z-4: re-reservation after release increments token preventing ABA", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-e";

  // Reserve → token=1
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 5000,
    now: 1000,
  });

  // Release (now status='released', token=1)
  commandStore.release(commandId, { fencingToken: 1 });

  // Re-reserve same commandId → token must be 2, not 1
  const rereservation = commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: 2000,
  });

  assert.equal(rereservation.created, true);
  assert.equal(rereservation.record.leaseToken, 2);
  assert.equal(rereservation.record.status, COMMAND_STATUSES.PROCESSING);
  assert.equal(rereservation.record.leaseOwner, "worker-2");

  // Old worker-1 with token=1 cannot mutate
  assert.throws(
    () => commandStore.recordEvent(commandId, createTestEvent({ commandId }), { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
  assert.throws(
    () => commandStore.complete(commandId, {}, { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
  assert.throws(
    () => commandStore.fail(commandId, { code: "X" }, { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
  assert.throws(
    () => commandStore.release(commandId, { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
});

// === Scenario F: healthy command exceeding TTL has defined safe behavior ===

test("Z-5: event store rejects append when lease is exactly expired (boundary)", () => {
  let currentTime = 1000;
  const { commandStore, eventStore } = createLeaseStores({ now: () => currentTime });

  const commandId = "cmd-f";
  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 1000,
    now: currentTime,
  });

  // Advance to exactly the expiry boundary
  currentTime = 2000; // leaseExpiresAt = 1000 + 1000 = 2000, now = 2000 → expired

  const event = createTestEvent({ commandId, aggregateId: 1, sequence: 1 });

  assert.throws(
    () => eventStore.append(event, { expectedVersion: 0, fencingToken: 1 }),
    (err) => err.code === "COMMAND_LEASE_EXPIRED"
  );
});

// === Scenario G: lease expiry during compensation ===

test("Z-5: lease expiry at boundary is consistent between takeover and event store", () => {
  let currentTime = 1000;
  const { commandStore, eventStore } = createLeaseStores({ now: () => currentTime });
  const commandId = "cmd-g";

  commandStore.reserve({
    commandId,
    commandType: "COMPENSATE",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 500,
    now: currentTime,
  });

  // At exactly lease expiry (1500), both takeover and append must agree
  currentTime = 1500;

  // Takeover should succeed (expired)
  const takeover = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: currentTime,
  });
  assert.equal(takeover.success, true);

  // Old worker with old token should not be able to append
  const event = createTestEvent({ commandId, aggregateId: 1, sequence: 1 });
  assert.throws(
    () => eventStore.append(event, { expectedVersion: 0, fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE" // token 1 is stale, current is 2
  );
});

// === Scenarios H-J: recordEvent/fail/complete after takeover with old token rejected ===

test("Z-1: recordEvent after takeover with old fencing token is rejected", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-h";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 100,
    now: 1000,
  });

  commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: 2000,
  });

  assert.throws(
    () => commandStore.recordEvent(commandId, createTestEvent({ commandId }), { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
});

test("Z-1: fail after takeover with old fencing token is rejected", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-i";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 100,
    now: 1000,
  });

  commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: 2000,
  });

  assert.throws(
    () => commandStore.fail(commandId, { code: "ERROR" }, { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
});

test("Z-1: complete after takeover with old fencing token is rejected", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-j";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 100,
    now: 1000,
  });

  commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: 2000,
  });

  assert.throws(
    () => commandStore.complete(commandId, {}, { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
});

// === Scenario K: missing command row + fencing token triggers FENCING_CONTEXT_INVALID ===

test("Z-3: event store rejects append with fencing token when command row is missing", () => {
  let currentTime = 1000;
  const { eventStore } = createLeaseStores({ now: () => currentTime });

  // Do NOT reserve any command — simulate a deleted/missing row
  const commandId = "cmd-k-missing";
  const event = createTestEvent({ commandId, aggregateId: 1, sequence: 1 });

  // Append with a fencing token but no command row → must reject
  assert.throws(
    () => eventStore.append(event, { expectedVersion: 0, fencingToken: 1 }),
    (err) => err.code === "FENCING_CONTEXT_INVALID"
  );
});

test("Z-3: event store allows append without fencing token when command row is missing", () => {
  let currentTime = 1000;
  const { eventStore } = createLeaseStores({ now: () => currentTime });

  const commandId = "cmd-k-no-token";
  const event = createTestEvent({ commandId, aggregateId: 1, sequence: 1 });

  // Append without fencing token — backward compatible, should succeed
  const stored = eventStore.append(event, { expectedVersion: 0 });
  assert.equal(stored.eventId, event.eventId);
});

// === Scenario L: now == leaseExpiresAt boundary consistency ===

test("Z-5: at exact expiry boundary, event store and takeover agree lease is expired", () => {
  const expiryTime = 5000;
  let currentTime = 1000;
  const { commandStore, eventStore } = createLeaseStores({ now: () => currentTime });
  const commandId = "cmd-l";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: expiryTime - 1000, // expires at 5000
    now: currentTime,
  });

  // Set to exact expiry
  currentTime = expiryTime;

  // Takeover: should see as expired (expiresAt > now fails since 5000 > 5000 is false)
  const takeover = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: currentTime,
  });
  assert.equal(takeover.success, true, "takeover should succeed at exact expiry");

  // Event store: old token should be stale (takeover bumped it)
  const event = createTestEvent({ commandId, aggregateId: 1, sequence: 1 });
  assert.throws(
    () => eventStore.append(event, { expectedVersion: 0, fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE",
    "old token should be stale after takeover"
  );
});

// === Additional: double takeover token monotonicity ===

test("double takeover produces monotonically increasing fencing tokens", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-double-takeover";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 100,
    now: 1000,
  });

  // First takeover: token 1 → 2
  const take1 = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-2",
    leaseTtlMs: 100,
    now: 2000,
  });
  assert.equal(take1.success, true);
  assert.equal(take1.record.leaseToken, 2);

  // Second takeover: token 2 → 3
  const take2 = commandStore.takeOverExpired({
    commandId,
    workerId: "worker-3",
    leaseTtlMs: 100,
    now: 3000,
  });
  assert.equal(take2.success, true);
  assert.equal(take2.record.leaseToken, 3);

  // All previous tokens are stale
  assert.throws(
    () => commandStore.recordEvent(commandId, createTestEvent({ commandId }), { fencingToken: 1 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
  assert.throws(
    () => commandStore.recordEvent(commandId, createTestEvent({ commandId }), { fencingToken: 2 }),
    (err) => err.code === "FENCING_TOKEN_STALE"
  );
});

// === G2: naming a generation is mandatory, not opt-in ===

test("mutating a command without a fencing token is rejected, not treated as unfenced", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-compat";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 5000,
    now: 1000,
  });

  // The command is otherwise perfectly releasable: status is processing, the
  // lease is unexpired and no events were committed. The only defect is the
  // missing generation, so that has to be the reason for the rejection.
  assert.throws(
    () => commandStore.release(commandId),
    (err) => err.code === "FENCING_TOKEN_REQUIRED"
  );
  assert.equal(commandStore.get(commandId).status, COMMAND_STATUSES.PROCESSING);

  // Naming the current generation releases the same command.
  assert.equal(commandStore.release(commandId, { fencingToken: 1 }), true);
  assert.equal(commandStore.get(commandId).status, COMMAND_STATUSES.RELEASED);
});

// === Re-reservation conflict on released command ===

test("re-reservation of released command with different payload reports conflict", () => {
  const { commandStore } = createLeaseStores();
  const commandId = "cmd-conflict";

  commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "X", quantity: 1 },
    workerId: "worker-1",
    leaseTtlMs: 5000,
    now: 1000,
  });

  commandStore.release(commandId, { fencingToken: 1 });

  // Try to re-reserve with different payload
  const result = commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Y", quantity: 2 },
    workerId: "worker-2",
    leaseTtlMs: 5000,
    now: 2000,
  });

  assert.equal(result.created, false);
  assert.equal(result.conflict, true);
});
