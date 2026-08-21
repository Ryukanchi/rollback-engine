const { describe, test } = require("node:test");
const assert = require("node:assert/strict");

const { EVENT_TYPES, createDomainEvent } = require("../../src/domain/events");

/**
 * Lease time belongs to the command store, so a contract test cannot hand a
 * chosen `now` to a single mutation. It positions the store's own clock at the
 * moment it means, and the store decides from there. Both adapters are held to
 * this identically.
 */
const leaseClocks = new WeakMap();

function createClockedStore(createStore) {
  const clock = { ms: 1000 };
  const store = createStore({ now: () => clock.ms });
  leaseClocks.set(store, clock);
  return store;
}

/** Positions `store`'s lease clock at `ms`, then hands the store back. */
function at(store, ms) {
  const clock = leaseClocks.get(store);
  if (!clock) {
    throw new Error("this store has no test-owned lease clock to position");
  }
  clock.ms = ms;
  return store;
}

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

function receiptMetadata({
  domainEffect = "events",
  aggregateId = 1,
  sequence = 1,
  lastEventId = "event-1-1",
} = {}) {
  return {
    contractVersion: 1,
    domainEffect,
    stateAnchor: {
      aggregateId,
      sequence,
      lastEventId,
    },
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
      const reconciliationRead =
        store.getRawByCommandIdForReconciliation("command-a");

      aggregateRead.length = 0;
      commandRead[0].payload.item = "Changed output";
      reconciliationRead[0].payload.item = "Changed raw output";

      assert.equal(store.getByAggregateId(1)[0].payload.item, "Pizza");
      assert.equal(store.getByCommandId("command-a")[0].payload.item, "Pizza");
      assert.equal(
        store.getRawByCommandIdForReconciliation("command-a")[0].payload.item,
        "Pizza"
      );
      assert.deepEqual(store.getByCommandId("missing-command"), []);
      assert.deepEqual(
        store.getRawByCommandIdForReconciliation("missing-command"),
        []
      );
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
      assert.equal(first.record.receiptMetadata, null);
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
      const metadata = receiptMetadata();
      const failure = { code: "DOMAIN_REJECTION", message: "Rejected" };

      completedStore.reserve(commandDescriptor("completed-command"));
      failedStore.reserve(commandDescriptor("failed-command"));
      completedStore.complete("completed-command", result, {
        fencingToken: 1,
        receiptMetadata: metadata,
      });
      failedStore.fail("failed-command", failure, { fencingToken: 1 });
      result.state.lifecycle = "Changed input";
      metadata.stateAnchor.lastEventId = "Changed input";
      failure.code = "Changed input";

      const completed = completedStore.get("completed-command");
      assert.equal(completed.result.state.lifecycle, "completed");
      assert.deepEqual(completed.receiptMetadata, receiptMetadata());
      completed.receiptMetadata.stateAnchor.lastEventId = "Changed output";
      assert.deepEqual(
        completedStore.get("completed-command").receiptMetadata,
        receiptMetadata()
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
          { fencingToken: 1, receiptMetadata: receiptMetadata() }
        )
      );
      assert.equal(store.get("command-1").status, "processing");
      assert.equal(store.get("command-1").result, null);
      assert.equal(store.get("command-1").receiptMetadata, null);
    });

    test("requires a structurally valid receipt contract before completion", () => {
      const store = createStore();
      store.reserve(commandDescriptor("receipt-contract"));

      const invalidMetadata = [
        undefined,
        { ...receiptMetadata(), contractVersion: 2 },
        { ...receiptMetadata(), domainEffect: "unknown" },
        { ...receiptMetadata(), stateAnchor: { aggregateId: 1, sequence: -1, lastEventId: null } },
        { ...receiptMetadata(), stateAnchor: { aggregateId: 1, sequence: 1, lastEventId: null } },
      ];

      for (const receiptMetadata of invalidMetadata) {
        assert.throws(() =>
          store.complete(
            "receipt-contract",
            { aggregateId: 1, state: { version: 1 } },
            { fencingToken: 1, receiptMetadata }
          )
        );
        assert.equal(store.get("receipt-contract").status, "processing");
        assert.equal(store.get("receipt-contract").result, null);
        assert.equal(store.get("receipt-contract").receiptMetadata, null);
      }
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
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("revoke-cmd");
      const error = { code: "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT" };

      assert.deepEqual(
        at(store, 9000).revokeExpired({ commandId: "absent-cmd", expectedToken: 1, error }),
        { success: false, reason: "NOT_FOUND" }
      );

      at(store, 1000).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000 });

      // Still inside the lease: a third party may not revoke yet.
      assert.deepEqual(
        at(store, 1500).revokeExpired({ commandId: descriptor.commandId, expectedToken: 1, error }),
        { success: false, reason: "NOT_EXPIRED" }
      );

      // Expired, but naming a generation that is not the current one.
      assert.deepEqual(
        at(store, 9000).revokeExpired({ commandId: descriptor.commandId, expectedToken: 99, error }),
        { success: false, reason: "TOKEN_MISMATCH" }
      );

      // A terminal command is no longer revocable.
      store.complete(descriptor.commandId, { ok: true }, {
        fencingToken: 1,
        receiptMetadata: receiptMetadata(),
      });
      assert.deepEqual(
        at(store, 9000).revokeExpired({ commandId: descriptor.commandId, expectedToken: 1, error }),
        { success: false, reason: "NOT_PROCESSING" }
      );

      // Argument contract.
      assert.throws(() =>
        at(store, 9000).revokeExpired({ commandId: descriptor.commandId, error })
      );
      assert.throws(() =>
        at(store, 9000).revokeExpired({ commandId: descriptor.commandId, expectedToken: 1 })
      );
    });

    test("allows safe takeover only when processing, expired, and without committed events", () => {
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("takeover-cmd");

      // Worker A reserves with lease TTL 1000ms at now = 1000
      const initial = at(store, 1000).reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1000,
      });
      assert.equal(initial.created, true);
      assert.equal(initial.record.leaseOwner, "worker-a");
      assert.equal(initial.record.leaseToken, 1);
      assert.equal(initial.record.leaseExpiresAt, 2000);

      // Attempt takeover before expiry (at now = 1500)
      const earlyTakeover = at(store, 1500).takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: 1000,
      });
      assert.equal(earlyTakeover.success, false);
      assert.equal(earlyTakeover.reason, "NOT_EXPIRED");

      // Attempt takeover after expiry (at now = 2001)
      const validTakeover = at(store, 2001).takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: 1000,
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
      const postEventTakeover = at(store, 5000).takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-c",
        leaseTtlMs: 1000,
      });
      assert.equal(postEventTakeover.success, false);
      assert.equal(postEventTakeover.reason, "HAS_EVENTS");
    });

    test("renews lease only for active owner and valid fencing token", () => {
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("renewal-cmd");

      at(store, 1000).reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1000,
      });

      // Valid renewal by Worker A
      const renewed = at(store, 1500).renewLease({
        commandId: descriptor.commandId,
        workerId: "worker-a",
        fencingToken: 1,
        leaseTtlMs: 2000,
      });
      assert.equal(renewed.renewed, true);
      assert.equal(renewed.leaseExpiresAt, 3500);

      // Impostor Worker B renewal rejected
      assert.throws(
        () =>
          at(store, 2000).renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-b",
            fencingToken: 1,
            leaseTtlMs: 1000,
          }),
        (err) => err.code === "FENCING_TOKEN_STALE"
      );

      // Stale fencing token rejected
      assert.throws(
        () =>
          at(store, 2000).renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-a",
            fencingToken: 99,
            leaseTtlMs: 1000,
          }),
        (err) => err.code === "FENCING_TOKEN_STALE"
      );

      // Naming no generation at all is rejected too. Renewal operates on an
      // existing generation, so owner identity alone carries no authority.
      assert.throws(
        () =>
          at(store, 2000).renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-a",
            leaseTtlMs: 1000,
          }),
        (err) => err.code === "FENCING_TOKEN_REQUIRED"
      );
      assert.equal(store.get(descriptor.commandId).leaseExpiresAt, 3500);
    });

    test("fences complete and fail transitions against stale fencing tokens", () => {
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("fencing-test-cmd");

      at(store, 1000).reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1000,
      });

      // Worker B takes over after expiration
      const takeover = at(store, 2500).takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: 2000,
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
        { fencingToken: 2, receiptMetadata: receiptMetadata() }
      );
      assert.equal(completed.status, "completed");
      assert.equal(completed.leaseOwner, null);
      assert.equal(completed.leaseExpiresAt, null);
    });

    // =====================================================================
    // Lease policy. A lease duration is caller-supplied execution policy, but
    // a malformed one must never become a persisted deadline: the two
    // adapters coerce stray JavaScript values differently, so a value that
    // survives validation stops having one numeric meaning. Both adapters are
    // therefore held to the identical contract here, and every refusal has to
    // leave the command exactly as it was.
    // =====================================================================
    const INVALID_LEASE_TTLS = [
      ["null", null],
      ["0", 0],
      ["-1", -1],
      ["0.5", 0.5],
      ["NaN", NaN],
      ["Infinity", Infinity],
      ["-Infinity", -Infinity],
      ['"5000"', "5000"],
      ['""', ""],
      ["true", true],
      ["{}", {}],
      ["[]", []],
    ];

    test("LT-1: reserve refuses every malformed lease duration and creates nothing", () => {
      for (const [label, leaseTtlMs] of INVALID_LEASE_TTLS) {
        const store = createClockedStore(createStore);
        const descriptor = commandDescriptor("ttl-reserve");

        assert.throws(
          () => at(store, 1000).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs }),
          TypeError,
          `reserve must refuse a lease duration of ${label}`
        );
        assert.equal(
          store.get(descriptor.commandId),
          null,
          `a refused lease duration of ${label} must not create a command`
        );
      }
    });

    test("LT-1: renewLease refuses every malformed lease duration and changes nothing", () => {
      for (const [label, leaseTtlMs] of INVALID_LEASE_TTLS) {
        const store = createClockedStore(createStore);
        const descriptor = commandDescriptor("ttl-renew");
        at(store, 1000).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000 });
        const before = store.get(descriptor.commandId);

        assert.throws(
          () =>
            at(store, 1500).renewLease({
              commandId: descriptor.commandId,
              workerId: "worker-a",
              fencingToken: 1,
              leaseTtlMs,
            }),
          TypeError,
          `renewLease must refuse a lease duration of ${label}`
        );
        assert.deepEqual(
          store.get(descriptor.commandId),
          before,
          `a refused renewal of ${label} must leave the row untouched`
        );
      }
    });

    test("LT-1: takeOverExpired refuses every malformed lease duration and changes nothing", () => {
      for (const [label, leaseTtlMs] of INVALID_LEASE_TTLS) {
        const store = createClockedStore(createStore);
        const descriptor = commandDescriptor("ttl-takeover");
        // Processing, expired, zero authoritative events, generation 1: the
        // takeover would otherwise be granted, so only the policy can stop it.
        at(store, 1000).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000 });
        const before = store.get(descriptor.commandId);

        assert.throws(
          () =>
            at(store, 9000).takeOverExpired({
              commandId: descriptor.commandId,
              workerId: "worker-b",
              leaseTtlMs,
              expectedToken: 1,
            }),
          TypeError,
          `takeOverExpired must refuse a lease duration of ${label}`
        );
        assert.deepEqual(
          store.get(descriptor.commandId),
          before,
          `a refused takeover of ${label} must not move the generation, owner or deadline`
        );
      }
    });

    test("LT-1: an omitted lease duration takes the default, but null is refused", () => {
      const store = createClockedStore(createStore);

      // `undefined` is what a destructuring default is for; `null` is not.
      const omitted = at(store, 1000).reserve({
        ...commandDescriptor("ttl-omitted"),
        workerId: "worker-a",
      });
      assert.equal(omitted.record.leaseExpiresAt, 6000, "the store default still applies");

      assert.throws(
        () =>
          at(store, 1000).reserve({
            ...commandDescriptor("ttl-null"),
            workerId: "worker-a",
            leaseTtlMs: null,
          }),
        TypeError
      );
      assert.equal(store.get("ttl-null"), null);
    });

    test("LT-1: one millisecond is a valid lease duration", () => {
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("ttl-one-ms");

      const reserved = at(store, 1000).reserve({
        ...descriptor,
        workerId: "worker-a",
        leaseTtlMs: 1,
      });

      assert.equal(reserved.record.leaseExpiresAt, 1001);
      assert.equal(store.get(descriptor.commandId).leaseExpiresAt, 1001);
    });

    // The load-bearing case: a duration can be a perfectly good positive safe
    // integer while `now + duration` is not. Validating the duration alone
    // lets an unrepresentable deadline reach persistence, where SQLite accepts
    // the write and then cannot read the row back at all.
    test("LT-2: reserve refuses a duration whose deadline is not a safe integer", () => {
      const now = 1000;
      const maxSafeTtl = Number.MAX_SAFE_INTEGER - now;
      assert.ok(Number.isSafeInteger(maxSafeTtl + 1), "the refused duration is itself a safe integer");

      const accepting = createClockedStore(createStore);
      const ok = commandDescriptor("deadline-max-safe");
      const reserved = at(accepting, now).reserve({
        ...ok,
        workerId: "worker-a",
        leaseTtlMs: maxSafeTtl,
      });
      assert.equal(reserved.record.leaseExpiresAt, Number.MAX_SAFE_INTEGER);
      assert.equal(
        accepting.get(ok.commandId).leaseExpiresAt,
        Number.MAX_SAFE_INTEGER,
        "the largest representable deadline must survive a round trip"
      );

      const refusing = createClockedStore(createStore);
      const bad = commandDescriptor("deadline-overflow");
      assert.throws(
        () =>
          at(refusing, now).reserve({
            ...bad,
            workerId: "worker-a",
            leaseTtlMs: maxSafeTtl + 1,
          }),
        TypeError
      );
      assert.equal(refusing.get(bad.commandId), null, "no unreadable row may be written");
    });

    // Re-reserving a released command is the one reserve path that rewrites an
    // existing row in place rather than inserting a new one. A policy refused
    // after that rewrite began would leave the command processing, with the
    // generation already advanced and no deadline at all - the shape that is
    // never challengeable again. The refusal therefore has to happen before the
    // first field is touched, and this is what pins that ordering.
    test("LT-6: a refused re-reservation leaves a released command untouched", () => {
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("ttl-released-rereserve");

      at(store, 1000).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000 });
      assert.equal(store.release(descriptor.commandId, { fencingToken: 1 }), true);

      const released = store.get(descriptor.commandId);
      assert.equal(released.status, "released");
      assert.equal(released.leaseToken, 1);
      assert.equal(released.leaseExpiresAt, null);

      for (const [label, leaseTtlMs] of [
        // Refused by the duration check, before the store reads its clock.
        ["a malformed duration", NaN],
        // Refused only by the deadline check: the duration itself is a
        // perfectly good positive safe integer, so this is the case that
        // actually depends on where the deadline is built.
        ["a duration whose deadline overflows", Number.MAX_SAFE_INTEGER - 1000 + 1],
      ]) {
        assert.throws(
          () =>
            at(store, 1000).reserve({
              ...descriptor,
              workerId: "worker-b",
              leaseTtlMs,
            }),
          TypeError,
          `re-reserving a released command with ${label} must be refused`
        );
        assert.deepEqual(
          store.get(descriptor.commandId),
          released,
          `${label}: a refused re-reservation must not rewrite the released row`
        );
      }

      // The row is still genuinely re-reservable, so the refusals above were
      // about the policy and not about the command having become unusable.
      const reReserved = at(store, 1000).reserve({
        ...descriptor,
        workerId: "worker-b",
        leaseTtlMs: 1000,
      });
      assert.equal(reReserved.created, true);
      assert.equal(reReserved.record.leaseToken, 2);
      assert.equal(reReserved.record.leaseExpiresAt, 2000);
    });

    test("LT-2: renewLease refuses a duration whose deadline is not a safe integer", () => {
      const now = 1000;
      const maxSafeTtl = Number.MAX_SAFE_INTEGER - now;
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("deadline-renew");
      at(store, now).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000 });
      const before = store.get(descriptor.commandId);

      assert.throws(
        () =>
          at(store, now).renewLease({
            commandId: descriptor.commandId,
            workerId: "worker-a",
            fencingToken: 1,
            leaseTtlMs: maxSafeTtl + 1,
          }),
        TypeError
      );
      assert.deepEqual(store.get(descriptor.commandId), before);

      const renewed = at(store, now).renewLease({
        commandId: descriptor.commandId,
        workerId: "worker-a",
        fencingToken: 1,
        leaseTtlMs: maxSafeTtl,
      });
      assert.equal(renewed.leaseExpiresAt, Number.MAX_SAFE_INTEGER);
    });

    test("LT-2: takeOverExpired refuses a duration whose deadline is not a safe integer", () => {
      const takeoverAt = 9000;
      const maxSafeTtl = Number.MAX_SAFE_INTEGER - takeoverAt;
      const store = createClockedStore(createStore);
      const descriptor = commandDescriptor("deadline-takeover");
      at(store, 1000).reserve({ ...descriptor, workerId: "worker-a", leaseTtlMs: 1000 });
      const before = store.get(descriptor.commandId);

      assert.throws(
        () =>
          at(store, takeoverAt).takeOverExpired({
            commandId: descriptor.commandId,
            workerId: "worker-b",
            leaseTtlMs: maxSafeTtl + 1,
            expectedToken: 1,
          }),
        TypeError
      );
      assert.deepEqual(store.get(descriptor.commandId), before);

      const takeover = at(store, takeoverAt).takeOverExpired({
        commandId: descriptor.commandId,
        workerId: "worker-b",
        leaseTtlMs: maxSafeTtl,
        expectedToken: 1,
      });
      assert.equal(takeover.success, true);
      assert.equal(takeover.record.leaseExpiresAt, Number.MAX_SAFE_INTEGER);
    });

    // The deadline contract has two operands, so it also protects the store
    // from its own clock. This is not clock hardening: it is the same rule.
    test("LT-2: a clock that cannot yield a safe deadline blocks the lease", () => {
      for (const [label, reading] of [
        ["NaN", NaN],
        ["Infinity", Infinity],
        ['the string "1000"', "1000"],
        ["an unsafe integer", Number.MAX_SAFE_INTEGER + 10],
        ["a fractional millisecond", 1000.5],
      ]) {
        const store = createClockedStore(createStore);
        const descriptor = commandDescriptor("clock-guard");

        assert.throws(
          () =>
            at(store, reading).reserve({
              ...descriptor,
              workerId: "worker-a",
              leaseTtlMs: 5000,
            }),
          TypeError,
          `a clock reading of ${label} must not become a deadline`
        );
        assert.equal(store.get(descriptor.commandId), null);
      }
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
    // =====================================================================
    // Materialized view write fencing. Every engine write to the view is a
    // conditional replace against the state that writer actually observed, so
    // a writer that fell behind loses instead of overwriting. The condition is
    // the observed state's identity, never its version: authoritative repair
    // must stay free to move the view backwards when replay says so.
    // =====================================================================
    test("MV-C2: compareAndSwap applies only when the view still matches the observed state", () => {
      const repository = createRepository();
      const observed = state(1, 1, "Pizza");
      const next = state(1, 2, "Pasta");
      repository.save(observed);

      assert.deepEqual(
        repository.compareAndSwap({ aggregateId: 1, expectedState: observed, nextState: next }),
        { applied: true }
      );
      assert.deepEqual(repository.getByAggregateId(1), next);
    });

    test("MV-C2: compareAndSwap loses when the view changed after observation", () => {
      const repository = createRepository();
      const observed = state(1, 1, "Pizza");
      repository.save(observed);
      const advancedByAnotherWriter = state(1, 2, "Pasta");
      repository.replace(advancedByAnotherWriter);

      const outcome = repository.compareAndSwap({
        aggregateId: 1,
        expectedState: observed,          // what the losing writer saw
        nextState: state(1, 2, "Stale"),
      });

      assert.deepEqual(outcome, { applied: false }, "a stale writer must lose");
      assert.deepEqual(
        repository.getByAggregateId(1),
        advancedByAnotherWriter,
        "and must not have written anything"
      );
    });

    test("MV-C2: compareAndSwap inserts only while the view is still absent", () => {
      const repository = createRepository();
      const first = state(1, 1, "Pizza");

      assert.deepEqual(
        repository.compareAndSwap({ aggregateId: 1, expectedState: null, nextState: first }),
        { applied: true }
      );
      assert.deepEqual(repository.getByAggregateId(1), first);

      assert.deepEqual(
        repository.compareAndSwap({ aggregateId: 1, expectedState: null, nextState: state(1, 9, "Late") }),
        { applied: false },
        "a second insert-if-absent must lose"
      );
      assert.deepEqual(repository.getByAggregateId(1), first);
    });

    test("MV-C3: compareAndSwap allows authoritative downward repair", () => {
      const repository = createRepository();
      const corrupt = state(1, 8, "CORRUPT");
      repository.save(corrupt);

      // Replay says the truth is v3. Moving the view backwards is exactly what
      // repair is for, so nothing here may consult version ordering.
      const truth = state(1, 3, "Pizza");
      assert.deepEqual(
        repository.compareAndSwap({ aggregateId: 1, expectedState: corrupt, nextState: truth }),
        { applied: true }
      );
      assert.deepEqual(repository.getByAggregateId(1), truth);
    });

    test("MV-C6: compareAndSwap repairs an equal-version, different-state corruption", () => {
      const repository = createRepository();
      const corrupt = state(1, 4, "CORRUPT");
      repository.save(corrupt);

      const truth = state(1, 4, "Pizza");   // same version, deterministic replay says this
      assert.deepEqual(
        repository.compareAndSwap({ aggregateId: 1, expectedState: corrupt, nextState: truth }),
        { applied: true }
      );
      assert.deepEqual(repository.getByAggregateId(1).order.item, "Pizza");
    });

    test("MV-C2: a lost compareAndSwap is an outcome, not an error", () => {
      const repository = createRepository();
      repository.save(state(1, 1, "Pizza"));

      // Losing a race is ordinary concurrency, so it must not throw. Malformed
      // arguments still must.
      assert.doesNotThrow(() =>
        repository.compareAndSwap({ aggregateId: 1, expectedState: state(1, 1, "Other"), nextState: state(1, 2) })
      );
      assert.throws(() =>
        repository.compareAndSwap({ aggregateId: 1, expectedState: null, nextState: { aggregateId: 1 } })
      );
      assert.throws(() =>
        repository.compareAndSwap({ aggregateId: 2, expectedState: null, nextState: state(1, 1) })
      );
    });

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
