const { describe, test } = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../../src/domain/events");

function createEvent({
  eventId,
  aggregateId = 1,
  sequence = 1,
  commandId = "command-1",
  timestamp = `2026-08-15T10:00:0${sequence}.000Z`,
} = {}) {
  const factsBySequence = {
    1: {
      eventType: EVENT_TYPES.ORDER_CREATED,
      payload: { item: "Pizza", quantity: 1 },
    },
    2: {
      eventType: EVENT_TYPES.INVENTORY_RESERVED,
      payload: { reservationId: 10, item: "Pizza", quantity: 1 },
    },
    3: {
      eventType: EVENT_TYPES.PAYMENT_CHARGED,
      payload: { paymentId: 20, amount: 100 },
    },
  };
  const fact = factsBySequence[sequence];

  if (!fact) {
    throw new Error(`No contract event fixture for sequence ${sequence}`);
  }

  return createDomainEvent({
    eventId: eventId ?? `event-${aggregateId}-${sequence}`,
    eventType: fact.eventType,
    aggregateId,
    sequence,
    timestamp,
    payload: fact.payload,
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: `correlation-${commandId}`,
      causationId: `cause-${commandId}-${sequence}`,
    },
  });
}

function commandDescriptor(commandId = "command-1") {
  return {
    commandId,
    commandType: "CHECKOUT",
    payload: { item: "Pizza", quantity: 1, amount: 100 },
  };
}

function snapshot(aggregateId, version, item = "Pizza") {
  return {
    aggregateId,
    version,
    timestamp: `2026-08-15T10:00:0${version}.000Z`,
    state: {
      aggregateId,
      version,
      lifecycle: "active",
      order: { item, quantity: 1 },
    },
  };
}

function state(aggregateId, version = 1, item = "Pizza") {
  return {
    aggregateId,
    version,
    lifecycle: "active",
    order: { item, quantity: 1 },
  };
}

function registerEventStoreContract({ adapterName, createStore }) {
  describe(`${adapterName} Event Store contract`, () => {
    test("appends by expected version and exposes consistent ordered reads", () => {
      const store = createStore();
      const first = createEvent({ sequence: 1, commandId: "command-a" });
      const second = createEvent({ sequence: 2, commandId: "command-a" });
      const otherAggregate = createEvent({
        aggregateId: 2,
        sequence: 1,
        commandId: "command-b",
      });

      assert.deepEqual(store.append(first, { expectedVersion: 0 }), first);
      assert.deepEqual(store.append(second, { expectedVersion: 1 }), second);
      store.append(otherAggregate, { expectedVersion: 0 });

      assert.deepEqual(store.getByAggregateId(1), [first, second]);
      assert.deepEqual(store.getByAggregateIdAfter(1, 0), [first, second]);
      assert.deepEqual(store.getByAggregateIdAfter(1, 1), [second]);
      assert.deepEqual(store.getByAggregateIdAfter(1, 2), []);
      assert.deepEqual(store.getByAggregateIdAfter(1, 99), []);
      assert.deepEqual(store.getByAggregateId(2), [otherAggregate]);
      assert.equal(store.getLastSequence(1), 2);
      assert.equal(store.getLastSequence(2), 1);
      assert.deepEqual(store.getAll(), [first, second, otherAggregate]);
    });

    test("rejects stale writers without mutating the aggregate stream", () => {
      const store = createStore();
      const first = createEvent({ sequence: 1 });
      const stale = createEvent({ sequence: 2 });

      store.append(first, { expectedVersion: 0 });

      assert.throws(
        () => store.append(stale, { expectedVersion: 0 }),
        (error) => {
          assert.equal(error.code, "OPTIMISTIC_CONCURRENCY_CONFLICT");
          assert.equal(error.aggregateId, 1);
          assert.equal(error.expectedVersion, 0);
          assert.equal(error.actualVersion, 1);
          return true;
        }
      );
      assert.deepEqual(store.getByAggregateId(1), [first]);
    });

    test("requires the writer to declare its observed aggregate version", () => {
      const store = createStore();

      assert.throws(() => store.append(createEvent({ sequence: 1 })));
      assert.deepEqual(store.getAll(), []);
    });

    test("requires contiguous sequences and globally unique event IDs", () => {
      const store = createStore();
      const first = createEvent({ eventId: "globally-unique", sequence: 1 });

      store.append(first, { expectedVersion: 0 });

      assert.throws(() =>
        store.append(createEvent({ aggregateId: 2, sequence: 1 }), {
          expectedVersion: 1,
        })
      );
      assert.throws(() =>
        store.append(
          createEvent({
            eventId: "globally-unique",
            aggregateId: 2,
            sequence: 1,
          }),
          { expectedVersion: 0 }
        )
      );
      assert.deepEqual(store.getByAggregateId(2), []);
      assert.equal(store.getAll().length, 1);
    });

    test("provides read-after-write command lookup and defensive values", () => {
      const store = createStore();
      const mutableEvent = structuredClone(
        createEvent({ eventId: "defensive-event", commandId: "command-a" })
      );

      store.append(mutableEvent, { expectedVersion: 0 });
      mutableEvent.payload.item = "Changed input";

      const aggregateRead = store.getByAggregateId(1);
      const commandRead = store.getByCommandId("command-a");

      aggregateRead.length = 0;
      commandRead[0].payload.item = "Changed output";

      assert.equal(store.getByAggregateId(1)[0].payload.item, "Pizza");
      assert.equal(store.getByCommandId("command-a")[0].payload.item, "Pizza");
      assert.deepEqual(store.getByCommandId("missing-command"), []);
    });

    test("accepts equal but rejects decreasing aggregate timestamps", () => {
      const store = createStore();
      const timestamp = "2026-08-15T10:00:01.000Z";

      store.append(createEvent({ sequence: 1, timestamp }), {
        expectedVersion: 0,
      });
      store.append(createEvent({ sequence: 2, timestamp }), {
        expectedVersion: 1,
      });

      assert.throws(() =>
        store.append(
          createEvent({
            sequence: 3,
            timestamp: "2026-08-15T10:00:00.000Z",
          }),
          { expectedVersion: 2 }
        )
      );
      assert.equal(store.getLastSequence(1), 2);
    });
  });
}

function registerCommandStoreContract({ adapterName, createStore }) {
  describe(`${adapterName} Command Store contract`, () => {
    test("reserves one normalized command identity exactly once", () => {
      const store = createStore();
      const descriptor = commandDescriptor();

      const first = store.reserve(descriptor);
      const repeated = store.reserve({
        ...descriptor,
        payload: { amount: 100, quantity: 1, item: "Pizza" },
      });
      const conflicting = store.reserve({
        ...descriptor,
        payload: { item: "Pasta", quantity: 1, amount: 100 },
      });

      assert.equal(first.created, true);
      assert.equal(first.record.status, "processing");
      assert.equal(repeated.created, false);
      assert.equal(repeated.conflict, false);
      assert.equal(conflicting.created, false);
      assert.equal(conflicting.conflict, true);
      assert.equal(store.get(descriptor.commandId).status, "processing");
    });

    test("tracks only one contiguous aggregate event range", () => {
      const store = createStore();
      const descriptor = commandDescriptor();
      const first = createEvent({ sequence: 1 });
      const second = createEvent({ sequence: 2 });

      store.reserve(descriptor);
      store.recordEvent(descriptor.commandId, first, { fencingToken: 1 });
      store.recordEvent(descriptor.commandId, second, { fencingToken: 1 });

      assert.deepEqual(store.get(descriptor.commandId).eventRange, {
        aggregateId: 1,
        firstSequence: 1,
        lastSequence: 2,
        eventIds: [first.eventId, second.eventId],
      });
      assert.throws(() =>
        store.recordEvent(
          descriptor.commandId,
          createEvent({ aggregateId: 2, sequence: 3 }),
          { fencingToken: 1 }
        )
      );
      assert.equal(store.get(descriptor.commandId).eventRange.lastSequence, 2);
    });

    test("completes or fails a processing command as one stable transition", () => {
      const completedStore = createStore();
      const failedStore = createStore();
      const result = { aggregateId: 1, state: { lifecycle: "completed" } };
      const failure = { code: "DOMAIN_REJECTION", message: "Rejected" };

      completedStore.reserve(commandDescriptor("completed-command"));
      failedStore.reserve(commandDescriptor("failed-command"));
      completedStore.complete("completed-command", result, { fencingToken: 1 });
      failedStore.fail("failed-command", failure, { fencingToken: 1 });
      result.state.lifecycle = "Changed input";
      failure.code = "Changed input";

      assert.equal(
        completedStore.get("completed-command").result.state.lifecycle,
        "completed"
      );
      assert.equal(failedStore.get("failed-command").error.code, "DOMAIN_REJECTION");
      assert.throws(() =>
        completedStore.fail("completed-command", { code: "TOO_LATE" }, { fencingToken: 1 })
      );
      assert.throws(() =>
        failedStore.complete("failed-command", { status: "too-late" }, { fencingToken: 1 })
      );
    });

    test("does not partially transition when result serialization fails", () => {
      const store = createStore();

      store.reserve(commandDescriptor());

      assert.throws(() =>
        store.complete(
          "command-1",
          { uncloneable: () => {} },
          { fencingToken: 1 }
        )
      );
      assert.equal(store.get("command-1").status, "processing");
      assert.equal(store.get("command-1").result, null);
    });

    test("releases only commands proven to have no committed event range", () => {
      const store = createStore();

      store.reserve(commandDescriptor("retryable-command"));
      assert.equal(store.release("retryable-command", { fencingToken: 1 }), true);
      const released = store.get("retryable-command");
      assert.equal(released.status, "released");
      assert.equal(released.leaseOwner, null);
      assert.equal(released.leaseExpiresAt, null);

      store.reserve(commandDescriptor("committed-command"));
      store.recordEvent(
        "committed-command",
        createEvent({ commandId: "committed-command" }),
        { fencingToken: 1 }
      );
      assert.throws(() => store.release("committed-command", { fencingToken: 1 }));
      assert.notEqual(store.get("committed-command"), null);
    });

    test("reconciles authoritative event ranges without exposing mutable records", () => {
      const store = createStore();
      const descriptor = commandDescriptor();
      const events = [createEvent({ sequence: 1 }), createEvent({ sequence: 2 })];

      store.reserve(descriptor);
      const reconciled = store.reconcileEvents(descriptor.commandId, events, {
        fencingToken: 1,
      });
      reconciled.eventRange.eventIds.length = 0;

      assert.deepEqual(store.get(descriptor.commandId).eventRange.eventIds, [
        events[0].eventId,
        events[1].eventId,
      ]);
    });

    test("reconciles or releases failed commands only under the recorded failure", () => {
      const releasableStore = createStore();
      const committedStore = createStore();

      releasableStore.reserve(commandDescriptor("releasable-command"));
      releasableStore.fail(
        "releasable-command",
        { code: "COMMAND_RECONCILIATION_FAILED" },
        { fencingToken: 1 }
      );

      assert.throws(() =>
        releasableStore.releaseFailed(
          "releasable-command",
          "EVENT_APPEND_COMMIT_UNKNOWN",
          { fencingToken: 1 }
        )
      );
      assert.equal(
        releasableStore.releaseFailed(
          "releasable-command",
          "COMMAND_RECONCILIATION_FAILED",
          { fencingToken: 1 }
        ),
        true
      );

      committedStore.reserve(commandDescriptor("committed-command"));
      committedStore.fail(
        "committed-command",
        { code: "EVENT_APPEND_COMMIT_UNKNOWN" },
        { fencingToken: 1 }
      );
      committedStore.reconcileFailure(
        "committed-command",
        [createEvent({ commandId: "committed-command" })],
        { code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" },
        { fencingToken: 1 }
      );

      const reconciled = committedStore.get("committed-command");

      assert.equal(reconciled.status, "failed");
      assert.equal(
        reconciled.error.code,
        "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT"
      );
      assert.deepEqual(reconciled.eventRange.eventIds, ["event-1-1"]);
      assert.throws(() =>
        committedStore.releaseFailed(
          "committed-command",
          "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
          { fencingToken: 1 }
        )
      );
    });

    test("revokes only a processing, correctly-generationed, still-expired command", () => {
      const store = createStore();
      const descriptor = commandDescriptor("revoke-cmd");
      const error = { code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" };

      assert.deepEqual(
        store.revokeExpired({ commandId: "absent-cmd", expectedToken: 1, now: 9000, error }),
        { success: false, reason: "NOT_FOUND" }
      );

      store.reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000, now: 1000 });

      // Still inside the lease: a third party may not revoke yet.
      assert.deepEqual(
        store.revokeExpired({ commandId: descriptor.commandId, expectedToken: 1, now: 1500, error }),
        { success: false, reason: "NOT_EXPIRED" }
      );

      // Expired, but naming a generation that is not the current one.
      assert.deepEqual(
        store.revokeExpired({ commandId: descriptor.commandId, expectedToken: 99, now: 9000, error }),
        { success: false, reason: "TOKEN_MISMATCH" }
      );

      // A terminal command is no longer revocable.
      store.complete(descriptor.commandId, { ok: true }, { fencingToken: 1 });
      assert.deepEqual(
        store.revokeExpired({ commandId: descriptor.commandId, expectedToken: 1, now: 9000, error }),
        { success: false, reason: "NOT_PROCESSING" }
      );

      // Argument contract.
      assert.throws(() =>
        store.revokeExpired({ commandId: descriptor.commandId, now: 9000, error })
      );
      assert.throws(() =>
        store.revokeExpired({ commandId: descriptor.commandId, expectedToken: 1, now: 9000 })
      );
    });

    test("allows safe takeover only when processing, expired, and without committed events", () => {
      const store = createStore();
      const descriptor = commandDescriptor("takeover-cmd");

      // Worker A reserves with lease TTL 1000ms at now = 1000
      const initial = store.reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1000,
        now: 1000,
      });
      assert.equal(initial.created, true);
      assert.equal(initial.record.leaseOwner, "worker-a");
      assert.equal(initial.record.leaseToken, 1);
      assert.equal(initial.record.leaseExpiresAt, 2000);

      // Attempt takeover before expiry (at now = 1500)
      const earlyTakeover = store.takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: 1000,
        now: 1500,
      });
      assert.equal(earlyTakeover.success, false);
      assert.equal(earlyTakeover.reason, "NOT_EXPIRED");

      // Attempt takeover after expiry (at now = 2001)
      const validTakeover = store.takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: 1000,
        now: 2001,
      });
      assert.equal(validTakeover.success, true);
      assert.equal(validTakeover.record.leaseOwner, "worker-b");
      assert.equal(validTakeover.record.leaseToken, 2);
      assert.equal(validTakeover.record.leaseExpiresAt, 3001);

      // Record an event under Worker B
      store.recordEvent(descriptor.commandId, createEvent({ commandId: descriptor.commandId }), {
        fencingToken: 2,
      });

      // Attempt takeover after events committed (even if expired at now = 5000)
      const postEventTakeover = store.takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-c",
        leaseTtlMs: 1000,
        now: 5000,
      });
      assert.equal(postEventTakeover.success, false);
      assert.equal(postEventTakeover.reason, "HAS_EVENTS");
    });

    test("renews lease only for active owner and valid fencing token", () => {
      const store = createStore();
      const descriptor = commandDescriptor("renewal-cmd");

      store.reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1000,
        now: 1000,
      });

      // Valid renewal by Worker A
      const renewed = store.renewLease({
        commandId: descriptor.commandId,
        workerId: "worker-a",
        fencingToken: 1,
        leaseTtlMs: 2000,
        now: 1500,
      });
      assert.equal(renewed.renewed, true);
      assert.equal(renewed.leaseExpiresAt, 3500);

      // Impostor Worker B renewal rejected
      assert.throws(
        () =>
          store.renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-b",
            fencingToken: 1,
            leaseTtlMs: 1000,
            now: 2000,
          }),
        (err) => err.code === "FENCING_TOKEN_STALE"
      );

      // Stale fencing token rejected
      assert.throws(
        () =>
          store.renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-a",
            fencingToken: 99,
            leaseTtlMs: 1000,
            now: 2000,
          }),
        (err) => err.code === "FENCING_TOKEN_STALE"
      );

      // Naming no generation at all is rejected too. Renewal operates on an
      // existing generation, so owner identity alone carries no authority.
      assert.throws(
        () =>
          store.renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-a",
            leaseTtlMs: 1000,
            now: 2000,
          }),
        (err) => err.code === "FENCING_TOKEN_REQUIRED"
      );
      assert.equal(store.get(descriptor.commandId).leaseExpiresAt, 3500);
    });

    test("fences complete and fail transitions against stale fencing tokens", () => {
      const store = createStore();
      const descriptor = commandDescriptor("fencing-test-cmd");

      store.reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1000,
        now: 1000,
      });

      // Worker B takes over after expiration
      const takeover = store.takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: 2000,
        now: 2500,
      });
      assert.equal(takeover.success, true);
      assert.equal(takeover.record.leaseToken, 2);

      // Worker A (zombie) tries to complete with stale token 1
      assert.throws(
        () =>
          store.complete(
            descriptor.commandId,
            { status: "completed" },
            { fencingToken: 1 }
          ),
        (err) => err.code === "FENCING_TOKEN_STALE"
      );

      // Worker A (zombie) tries to fail with stale token 1
      assert.throws(
        () =>
          store.fail(
            descriptor.commandId,
            { code: "ZOMBIE_FAIL", message: "Failed" },
            { fencingToken: 1 }
          ),
        (err) => err.code === "FENCING_TOKEN_STALE"
      );

      // Worker B completes successfully with token 2
      const completed = store.complete(
        descriptor.commandId,
        { status: "completed" },
        { fencingToken: 2 }
      );
      assert.equal(completed.status, "completed");
      assert.equal(completed.leaseOwner, null);
      assert.equal(completed.leaseExpiresAt, null);
    });
  });
}

function registerSnapshotStoreContract({ adapterName, createStore }) {
  describe(`${adapterName} Snapshot Store contract`, () => {
    test("stores independent versioned snapshots defensively", () => {
      const store = createStore();
      const first = snapshot(1, 1, "Pizza");
      const second = snapshot(2, 1, "Pasta");

      store.save(first);
      store.save(second);
      first.state.order.item = "Changed input";
      const loaded = store.getByAggregateId(1);
      loaded.state.order.item = "Changed output";

      assert.equal(store.getByAggregateId(1).state.order.item, "Pizza");
      assert.equal(store.getByAggregateId(2).state.order.item, "Pasta");
      assert.equal(store.getByAggregateId(999), null);
    });

    test("accepts newer versions and rejects stale replacements", () => {
      const store = createStore();
      const versionOne = snapshot(1, 1);
      const versionTwo = snapshot(1, 2);

      store.save(versionOne);
      store.save(versionTwo);

      assert.deepEqual(store.getByAggregateId(1), versionTwo);
      assert.throws(() => store.save(versionOne));
      assert.deepEqual(store.getByAggregateId(1), versionTwo);
    });

    test("is idempotent only for an equivalent snapshot at the same version", () => {
      const store = createStore();
      const original = snapshot(1, 1, "Pizza");
      const equivalent = structuredClone(original);
      const conflicting = snapshot(1, 1, "Pasta");

      store.save(original);

      assert.deepEqual(store.save(equivalent), equivalent);
      assert.throws(() => store.save(conflicting));
      assert.deepEqual(store.getByAggregateId(1), original);
    });

    test("rejects snapshots without a committed event version", () => {
      const store = createStore();

      assert.throws(() =>
        store.save({
          aggregateId: 1,
          version: 0,
          timestamp: "2026-08-15T10:00:00.000Z",
          state: { aggregateId: 1, version: 0 },
        })
      );
      assert.equal(store.getByAggregateId(1), null);
    });

    test("rejects snapshots whose state identity or version does not match", () => {
      const aggregateMismatchStore = createStore();
      const versionMismatchStore = createStore();
      const aggregateMismatch = snapshot(1, 1);
      const versionMismatch = snapshot(1, 1);

      aggregateMismatch.state.aggregateId = 2;
      versionMismatch.state.version = 2;

      assert.throws(() => aggregateMismatchStore.save(aggregateMismatch));
      assert.throws(() => versionMismatchStore.save(versionMismatch));
      assert.equal(aggregateMismatchStore.getByAggregateId(1), null);
      assert.equal(versionMismatchStore.getByAggregateId(1), null);
    });
  });
}

function registerStateRepositoryContract({ adapterName, createRepository }) {
  describe(`${adapterName} State Repository contract`, () => {
    test("separates save from replace and returns null for missing state", () => {
      const repository = createRepository();
      const initial = state(1, 1);
      const replacement = state(1, 2);

      repository.save(initial);

      assert.throws(() => repository.save(initial));
      assert.throws(() => repository.replace(state(2, 1)));
      assert.deepEqual(repository.replace(replacement), replacement);
      assert.deepEqual(repository.getByAggregateId(1), replacement);
      assert.equal(repository.getByAggregateId(999), null);
    });

    test("keeps aggregates isolated and exposes all materialized states", () => {
      const repository = createRepository();
      const first = state(1, 1, "Pizza");
      const second = state(2, 1, "Pasta");

      repository.save(first);
      repository.save(second);
      const listed = repository
        .getAll()
        .sort((left, right) => left.aggregateId - right.aggregateId);

      assert.deepEqual(repository.getByAggregateId(1), first);
      assert.deepEqual(repository.getByAggregateId(2), second);
      assert.deepEqual(listed, [first, second]);
    });

    test("isolates stored state from input and output mutations", () => {
      const repository = createRepository();
      const mutableState = state(1, 1);

      repository.save(mutableState);
      mutableState.order.item = "Changed input";
      const loaded = repository.getByAggregateId(1);
      loaded.order.item = "Changed output";
      const listed = repository.getAll();
      listed[0].order.item = "Changed list output";

      assert.equal(repository.getByAggregateId(1).order.item, "Pizza");
    });
  });
}

module.exports = {
  registerCommandStoreContract,
  registerEventStoreContract,
  registerSnapshotStoreContract,
  registerStateRepositoryContract,
};
