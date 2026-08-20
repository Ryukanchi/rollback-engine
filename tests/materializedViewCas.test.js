const test = require("node:test");
const { describe } = require("node:test");
const assert = require("node:assert/strict");
const { join } = require("node:path");
const { tmpdir } = require("node:os");
const { randomUUID } = require("node:crypto");

const { createStorageAdapters } = require("../src/infrastructure/storageFactory");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { projectEvents } = require("../src/domain/projection");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");

const PAYLOAD = { item: "Widget", quantity: 2, amount: 250 };

/**
 * Two workers. For SQLite they are genuinely separate connections on one file;
 * for memory they share one store set, which is the same sharing a single
 * process gives you there.
 */
function workers(storeType) {
  if (storeType === "sqlite") {
    const dbPath = join(tmpdir(), `mvcas-${randomUUID()}.db`);
    return { A: createStorageAdapters({ type: "sqlite", dbPath }), B: createStorageAdapters({ type: "sqlite", dbPath }) };
  }
  const A = createStorageAdapters({ type: "memory" });
  return { A, B: A };
}

function engineOn(adapters, workerId) {
  return new RollbackEngine({
    eventStore: adapters.eventStore,
    commandStore: adapters.commandStore,
    snapshotStore: adapters.snapshotStore,
    stateRepository: adapters.stateRepository,
    workerId,
  });
}

function close(...adapterSets) {
  for (const a of new Set(adapterSets)) {
    try { a.close(); } catch { /* already closed */ }
  }
}

/** The competing worker commits the next event and publishes the view it derives. */
function competitorAdvances(B, aggregateId, eventType, payload) {
  const events = B.eventStore.getByAggregateId(aggregateId);
  const event = createDomainEvent({
    eventId: `competitor-${randomUUID()}`,
    eventType,
    aggregateId,
    sequence: events.length + 1,
    timestamp: new Date().toISOString(),
    payload,
    metadata: { schemaVersion: 1, commandId: "competitor", correlationId: "competitor", causationId: "competitor" },
  });
  B.eventStore.append(event, { expectedVersion: events.length });
  const published = projectEvents(B.eventStore.getByAggregateId(aggregateId));
  const current = B.stateRepository.getByAggregateId(aggregateId);
  current ? B.stateRepository.replace(published) : B.stateRepository.save(published);
  return published;
}

const REFUND = { paymentId: 1, amount: 250, reason: "competing compensation" };
const DELETE = { reason: "competing delete" };
const RELEASE = { reservationId: 1, item: "Widget", quantity: 2 };

/** Fires `onFire` once, right after the engine reads an aggregate's events. */
function armAfterEventRead(adapters, onFire) {
  const fired = { count: 0, note: null };
  let armed = false;
  const real = adapters.eventStore.getByAggregateId.bind(adapters.eventStore);
  adapters.eventStore.getByAggregateId = (id) => {
    const events = real(id);
    if (armed) { armed = false; fired.count += 1; fired.note = onFire(); }
    return events;
  };
  return { fired, arm: () => { armed = true; } };
}

// ===========================================================================
// MV-C1 / MV-C8 - ordinary publication
// ===========================================================================
for (const storeType of ["memory", "sqlite"]) {
  describe(`Materialized view write fencing (${storeType})`, () => {
    test("MV-C1: a stale publication cannot overwrite a newer published view", () => {
      const { A, B } = workers(storeType);
      const engine = engineOn(A, "worker-A");

      let appends = 0;
      let competitor = null;
      const realRecord = A.commandStore.recordEvent.bind(A.commandStore);
      A.commandStore.recordEvent = (...args) => {
        const result = realRecord(...args);
        appends += 1;
        // The gap this test exists for: the event is durable, the projection
        // for it has not been published yet.
        if (appends === 3) competitor = competitorAdvances(B, 1, EVENT_TYPES.PAYMENT_REFUNDED, REFUND);
        return result;
      };

      const outcome = engine.checkout(PAYLOAD, { commandId: "cA" });

      // Anti-vacuity: the interleaving really happened.
      assert.equal(appends, 3, "worker A must have committed all three events");
      assert.notEqual(competitor, null, "the competing writer must have run inside the gap");
      assert.equal(competitor.version, 4, "the competitor must have published a strictly newer view");

      // MV-C8: losing a projection race is not a domain failure.
      assert.equal(outcome.status, "completed", "the committed command must still succeed");

      const truth = projectEvents(A.eventStore.getByAggregateId(1));
      const view = A.stateRepository.getByAggregateId(1);
      assert.equal(truth.version, 4, "event history is authoritative and unchanged");
      assert.notDeepEqual(view, projectEvents(A.eventStore.getByAggregateId(1).slice(0, 3)),
        "the stale v3 projection must not have been written");
      assert.ok(view.version >= 4, `the view must not move backwards, got v${view.version}`);
      close(A, B);
    });

    test("MV-C7: a stale publication cannot resurrect a deleted order", () => {
      const { A, B } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      let appends = 0;
      let competitor = null;
      const realRecord = A.commandStore.recordEvent.bind(A.commandStore);
      A.commandStore.recordEvent = (...args) => {
        const result = realRecord(...args);
        appends += 1;
        if (appends === 3) competitor = competitorAdvances(B, 1, EVENT_TYPES.ORDER_DELETED, DELETE);
        return result;
      };

      let outcome = null;
      let failure = null;
      try { outcome = engine.compensate(1, "worker A rollback", { commandId: "c2" }); }
      catch (error) { failure = error; }

      assert.notEqual(competitor, null, "the competing delete must have run inside the gap");
      assert.equal(competitor.deleted, true, "the competitor must have published a deleted view");
      assert.equal(failure, null, "losing the projection race must not fail the committed command");
      assert.notEqual(outcome, null, "the compensation must still return a result");

      const truth = projectEvents(A.eventStore.getByAggregateId(1));
      assert.equal(truth.deleted, true, "event history says the order is deleted");
      const view = A.stateRepository.getByAggregateId(1);
      assert.equal(view.deleted, true, "a stale writer must never resurrect a deleted order");
      assert.equal(engine.getOrder(1, { consistency: "materialized" }), null,
        "the default read must not serve a resurrected order");
      close(A, B);
    });

    // =====================================================================
    // MV-C4 - the repair paths are writers too and must be fenced identically
    // =====================================================================
    test("MV-C4: #ensureLiveState repair cannot clobber a concurrently published view", () => {
      const { A, B } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      // Leave the view stale so a repair is actually required.
      const events = A.eventStore.getByAggregateId(1);
      A.stateRepository.replace(projectEvents(events.slice(0, 2)));

      const { fired, arm } = armAfterEventRead(A, () =>
        competitorAdvances(B, 1, EVENT_TYPES.PAYMENT_REFUNDED, REFUND));
      arm();
      engine.getState(1, { consistency: "authoritative" });

      assert.equal(fired.count, 1, "the competitor must have run inside the repair window");
      assert.equal(fired.note.version, 4, "the competitor must have published a newer view");
      const truth = projectEvents(A.eventStore.getByAggregateId(1));
      const view = A.stateRepository.getByAggregateId(1);
      assert.equal(truth.version, 4);
      assert.ok(view.version >= 4, `repair must not write its stale replay, got v${view.version}`);
      close(A, B);
    });

    test("MV-C4: recover() cannot clobber a concurrently published view", () => {
      const { A, B } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      A.stateRepository.replace(projectEvents(A.eventStore.getByAggregateId(1).slice(0, 2)));

      const { fired, arm } = armAfterEventRead(A, () =>
        competitorAdvances(B, 1, EVENT_TYPES.PAYMENT_REFUNDED, REFUND));
      arm();
      engine.recover(1, { useSnapshot: false });

      assert.equal(fired.count, 1, "the competitor must have run inside the recover window");
      const truth = projectEvents(A.eventStore.getByAggregateId(1));
      const view = A.stateRepository.getByAggregateId(1);
      assert.equal(truth.version, 4);
      assert.ok(view.version >= 4, `recover must not write its stale replay, got v${view.version}`);
      close(A, B);
    });

    test("MV-C4: the #recordEvent repair fallback cannot clobber a concurrently published view", () => {
      const { A, B } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      // The next publication fails, which drops the engine into its repair
      // fallback; the competitor advances while that repair is in flight.
      let writes = 0;
      let competitor = null;
      for (const method of ["compareAndSwap", "replace", "save"]) {
        if (typeof A.stateRepository[method] !== "function") continue;
        const real = A.stateRepository[method].bind(A.stateRepository);
        A.stateRepository[method] = (...args) => {
          writes += 1;
          if (writes === 1) throw new Error("materialized view store unavailable");
          if (writes === 2 && competitor === null) {
            competitor = competitorAdvances(B, 1, EVENT_TYPES.INVENTORY_RELEASED, RELEASE);
          }
          return real(...args);
        };
      }

      try { engine.compensate(1, "worker A rollback", { commandId: "c2" }); } catch { /* repair may surface */ }

      assert.notEqual(competitor, null, "the competitor must have run inside the repair fallback");
      const truth = projectEvents(A.eventStore.getByAggregateId(1));
      const view = A.stateRepository.getByAggregateId(1);
      assert.ok(view.version >= competitor.version,
        `the repair fallback must not write a view older than the competitor's v${competitor.version}, got v${view.version}`);
      assert.ok(truth.version >= competitor.version, "event history stays authoritative");
      close(A, B);
    });

    test("MV-C5: a missing view is inserted once, and a late insert loses", () => {
      const { A, B } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      const truth = projectEvents(A.eventStore.getByAggregateId(1));

      A.stateRepository.reset();
      assert.equal(A.stateRepository.getByAggregateId(1), null, "the view starts missing");

      const first = A.stateRepository.compareAndSwap({ aggregateId: 1, expectedState: null, nextState: truth });
      const second = B.stateRepository.compareAndSwap({ aggregateId: 1, expectedState: null, nextState: { ...truth, version: 99 } });

      assert.deepEqual(first, { applied: true });
      assert.deepEqual(second, { applied: false }, "a late insert must lose rather than overwrite");
      assert.deepEqual(A.stateRepository.getByAggregateId(1), truth);
      close(A, B);
    });

    test("MV-C3: authoritative repair still moves a corrupt view backwards", () => {
      const { A } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      const truth = projectEvents(A.eventStore.getByAggregateId(1));

      A.stateRepository.replace({ ...truth, version: 8, order: { ...truth.order, item: "CORRUPT" } });
      assert.equal(A.stateRepository.getByAggregateId(1).version, 8);

      const repaired = engine.getState(1, { consistency: "authoritative" });

      assert.equal(repaired.version, truth.version, "repair must return replay truth");
      assert.deepEqual(A.stateRepository.getByAggregateId(1), truth, "v8 -> v3 downward repair must still work");
      close(A);
    });

    test("MV-C6: an equal-version corruption is repaired, and an untouched view is not rewritten", () => {
      const { A } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      const truth = projectEvents(A.eventStore.getByAggregateId(1));

      // same version, different state -> deterministic replay says one is wrong
      A.stateRepository.replace({ ...truth, order: { ...truth.order, item: "FORGED" } });
      engine.getState(1, { consistency: "authoritative" });
      assert.deepEqual(A.stateRepository.getByAggregateId(1), truth, "equal-version corruption must be repaired");

      // same version, same state -> no write at all
      let writes = 0;
      for (const method of ["compareAndSwap", "replace", "save"]) {
        if (typeof A.stateRepository[method] !== "function") continue;
        const real = A.stateRepository[method].bind(A.stateRepository);
        A.stateRepository[method] = (...args) => { writes += 1; return real(...args); };
      }
      engine.getState(1, { consistency: "authoritative" });
      assert.equal(writes, 0, "an already-correct view must not be rewritten");
      close(A);
    });

    test("MV-C6: a lost CAS is not blessed just because the stored view looks newer", () => {
      const { A } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      const truth = projectEvents(A.eventStore.getByAggregateId(1));

      // A corrupt view carrying a higher version than replay. Version ordering
      // must never be allowed to stand in for correctness here.
      A.stateRepository.replace({ ...truth, version: 8, order: { ...truth.order, item: "CORRUPT" } });

      let losses = 0;
      const real = A.stateRepository.compareAndSwap.bind(A.stateRepository);
      A.stateRepository.compareAndSwap = (args) => {
        if (losses === 0) { losses += 1; return { applied: false }; }   // lose once
        return real(args);
      };

      engine.getState(1, { consistency: "authoritative" });

      assert.equal(losses, 1, "the first attempt must actually have lost");
      assert.deepEqual(
        A.stateRepository.getByAggregateId(1),
        truth,
        "a higher stored version must not end the reconciliation as if it were correct"
      );
      close(A);
    });

    test("MV-C9: reconciliation after repeated CAS loss is bounded", () => {
      const { A } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      A.stateRepository.replace(projectEvents(A.eventStore.getByAggregateId(1).slice(0, 2)));

      // Every attempt loses and nothing is ever written, so the view stays
      // divergent and reconciliation can only stop because it is bounded.
      let attempts = 0;
      A.stateRepository.compareAndSwap = () => { attempts += 1; return { applied: false }; };

      let failure = null;
      assert.throws(
        () => engine.getState(1, { consistency: "authoritative" }),
        (error) => {
          failure = error;
          return error.code === "MATERIALIZED_VIEW_RECONCILIATION_EXHAUSTED";
        }
      );

      assert.ok(attempts > 1, "reconciliation must retry, not stop after the first loss");
      assert.ok(attempts <= 8, `reconciliation must be bounded, saw ${attempts} attempts`);
      assert.equal(failure.aggregateId, 1);
      assert.equal(
        failure.eventCommitted,
        undefined,
        "an authoritative read must not invent a command commit outcome"
      );
      assert.notDeepEqual(
        A.stateRepository.getByAggregateId(1),
        projectEvents(A.eventStore.getByAggregateId(1)),
        "anti-vacuity: the view must still be divergent, so every attempt really lost"
      );
      close(A);
    });

    // MV-C11 - exhausting repair must never promote the derived view into
    // domain authority. Every CAS loss below is genuine: a competing writer
    // changes the full stored identity before the conditional write runs.
    test("MV-C11: CAS exhaustion cannot authorize an invalid domain transition", (t) => {
      const { A } = workers(storeType);
      t.after(() => close(A));
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      const truthBefore = engine.replay(1);
      const eventIdsBefore = A.eventStore
        .getByAggregateId(1)
        .map((event) => event.eventId);

      // Same aggregate and version, but a false domain claim: the derived view
      // hides the active reservation and charge, which would make deletion look
      // legal if this cache were ever treated as authoritative.
      A.stateRepository.replace({
        ...truthBefore,
        lifecycle: "active",
        inventory: null,
        payment: null,
        corruptionNonce: 0,
      });

      let casLosses = 0;
      const realCompareAndSwap = A.stateRepository.compareAndSwap.bind(
        A.stateRepository
      );
      A.stateRepository.compareAndSwap = (args) => {
        casLosses += 1;
        const current = A.stateRepository.getByAggregateId(1);

        // Move the row after observation so the real reference-adapter CAS,
        // rather than a fake return value, decides that this writer lost.
        A.stateRepository.replace({
          ...current,
          corruptionNonce: casLosses,
        });

        return realCompareAndSwap(args);
      };

      let commandError = null;
      try {
        engine.deleteOrder(1, "adversarial delete", { commandId: "c2" });
      } catch (error) {
        commandError = error;
      }

      const eventsAfter = A.eventStore.getByAggregateId(1);
      let replayAfter = null;
      let replayError = null;
      try {
        replayAfter = engine.replay(1);
      } catch (error) {
        replayError = error;
      }

      // One aggregate assertion makes every required safety dimension visible
      // in the red-test diff. A mere thrown error is not sufficient: the event
      // log must be unchanged and replayable, and its domain state must remain
      // the pre-command truth.
      assert.deepEqual(
        {
          casWasActuallyContended: casLosses > 0,
          errorCode: commandError?.code,
          commandFailedBeforeCommit:
            commandError !== null && commandError.eventCommitted !== true,
          retrySafe: commandError?.retrySafe,
          retryAction: commandError?.retryAction,
          commandStatus: A.commandStore.get("c2")?.status,
          eventIds: eventsAfter.map((event) => event.eventId),
          invalidDeleteEvents: eventsAfter.filter(
            (event) => event.eventType === EVENT_TYPES.ORDER_DELETED
          ).length,
          replayable: replayError === null,
          replayState: replayAfter,
        },
        {
          casWasActuallyContended: true,
          errorCode: "MATERIALIZED_VIEW_RECONCILIATION_EXHAUSTED",
          commandFailedBeforeCommit: true,
          retrySafe: true,
          retryAction: "RETRY_SAME_KEY",
          commandStatus: "released",
          eventIds: eventIdsBefore,
          invalidDeleteEvents: 0,
          replayable: true,
          replayState: truthBefore,
        }
      );

      // Once contention ends, the released idempotency key is safe to retry.
      // Authoritative repair then reveals the charge/reservation and the normal
      // domain rejection wins without appending anything.
      A.stateRepository.compareAndSwap = realCompareAndSwap;
      assert.throws(
        () => engine.deleteOrder(1, "adversarial delete", { commandId: "c2" }),
        (error) =>
          error.code === "COMPENSATION_REQUIRED" &&
          error.eventCommitted === false &&
          error.retrySafe === false
      );
      assert.deepEqual(
        A.eventStore.getByAggregateId(1).map((event) => event.eventId),
        eventIdsBefore
      );
      assert.deepEqual(engine.replay(1), truthBefore);
    });

    test("MV-C12: authoritative reads fail closed while replay remains available", (t) => {
      const { A } = workers(storeType);
      t.after(() => close(A));
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      const truth = engine.replay(1);
      const corruptView = {
        ...truth,
        order: { ...truth.order, item: "FORGED" },
      };
      A.stateRepository.replace(corruptView);

      let attempts = 0;
      const realCompareAndSwap = A.stateRepository.compareAndSwap.bind(
        A.stateRepository
      );
      A.stateRepository.compareAndSwap = () => {
        attempts += 1;
        return { applied: false };
      };

      for (const read of [
        () => engine.getState(1, { consistency: "authoritative" }),
        () => engine.getOrder(1, { consistency: "authoritative" }),
      ]) {
        assert.throws(
          read,
          (error) =>
            error.code === "MATERIALIZED_VIEW_RECONCILIATION_EXHAUSTED" &&
            error.aggregateId === 1 &&
            error.eventCommitted === undefined
        );
      }

      assert.ok(attempts > 1, "both authoritative reads must reach bounded reconciliation");
      assert.deepEqual(engine.replay(1), truth, "authoritative replay remains available");
      assert.deepEqual(
        engine.getState(1, { consistency: "materialized" }),
        corruptView,
        "the explicit materialized read remains derived and may be stale"
      );
      A.stateRepository.compareAndSwap = realCompareAndSwap;
    });

    test("MV-C13: failed recovery reports no restore and succeeds after contention", (t) => {
      const { A } = workers(storeType);
      t.after(() => close(A));
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      const truth = engine.replay(1);
      const eventIds = A.eventStore
        .getByAggregateId(1)
        .map((event) => event.eventId);
      const staleView = projectEvents(A.eventStore.getByAggregateId(1).slice(0, 2));
      A.stateRepository.replace(staleView);

      const realCompareAndSwap = A.stateRepository.compareAndSwap.bind(
        A.stateRepository
      );
      A.stateRepository.compareAndSwap = () => ({ applied: false });

      assert.throws(
        () => engine.recover(1, { useSnapshot: false }),
        (error) =>
          error.code === "MATERIALIZED_VIEW_RECONCILIATION_EXHAUSTED" &&
          error.aggregateId === 1 &&
          error.eventCommitted === undefined
      );
      assert.deepEqual(
        A.eventStore.getByAggregateId(1).map((event) => event.eventId),
        eventIds,
        "recovery must never create domain events"
      );
      assert.deepEqual(engine.replay(1), truth);
      assert.deepEqual(A.stateRepository.getByAggregateId(1), staleView);

      A.stateRepository.compareAndSwap = realCompareAndSwap;
      const recovered = engine.recover(1, { useSnapshot: false });
      assert.deepEqual(recovered, truth);
      assert.deepEqual(A.stateRepository.getByAggregateId(1), truth);
      assert.deepEqual(
        A.eventStore.getByAggregateId(1).map((event) => event.eventId),
        eventIds
      );
    });

    test("MV-C14: post-commit CAS exhaustion preserves committed-error semantics", (t) => {
      const { A } = workers(storeType);
      t.after(() => close(A));
      const engine = engineOn(A, "worker-A");
      const command = { item: "Widget", quantity: 2 };
      const options = { commandId: "post-commit-cas" };

      let casLosses = 0;
      const realCompareAndSwap = A.stateRepository.compareAndSwap.bind(
        A.stateRepository
      );
      A.stateRepository.compareAndSwap = (args) => {
        casLosses += 1;
        const current = A.stateRepository.getByAggregateId(args.aggregateId);

        if (current) {
          A.stateRepository.replace({
            ...current,
            corruptionNonce: casLosses,
          });
        } else {
          A.stateRepository.save({
            ...args.nextState,
            order: { ...args.nextState.order, item: "CORRUPT" },
            corruptionNonce: casLosses,
          });
        }

        return realCompareAndSwap(args);
      };

      let committedError = null;
      assert.throws(
        () => engine.createOrder(command, options),
        (error) => {
          committedError = error;
          return (
            error.code === "EVENT_COMMITTED_VIEW_REPAIR_FAILED" &&
            error.eventCommitted === true &&
            error.retrySafe === false &&
            error.commandId === options.commandId
          );
        }
      );

      const eventsAfterFailure = A.eventStore.getByAggregateId(1);
      assert.ok(casLosses > 1, "publication and synchronous repair must both contend");
      assert.equal(eventsAfterFailure.length, 1);
      assert.equal(eventsAfterFailure[0].eventType, EVENT_TYPES.ORDER_CREATED);
      assert.equal(committedError.eventId, eventsAfterFailure[0].eventId);
      assert.equal(engine.replay(1).order.item, "Widget");
      assert.equal(A.stateRepository.getByAggregateId(1).order.item, "CORRUPT");
      assert.equal(A.commandStore.get(options.commandId).status, "failed");

      assert.throws(
        () => engine.createOrder(command, options),
        (error) =>
          error.code === "EVENT_COMMITTED_VIEW_REPAIR_FAILED" &&
          error.eventCommitted === true
      );
      assert.equal(
        A.eventStore.getByAggregateId(1).length,
        1,
        "replaying the idempotency key must not append the event again"
      );

      A.stateRepository.compareAndSwap = realCompareAndSwap;
      assert.deepEqual(engine.recover(1, { useSnapshot: false }), engine.replay(1));
      assert.deepEqual(A.stateRepository.getByAggregateId(1), engine.replay(1));
      assert.equal(A.eventStore.getByAggregateId(1).length, 1);
    });

    test("MV-C15: the final CAS may lose to a replay-valid competing repair", (t) => {
      const { A } = workers(storeType);
      t.after(() => close(A));
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });

      const truth = engine.replay(1);
      A.stateRepository.replace({
        ...truth,
        order: { ...truth.order, item: "CORRUPT" },
        corruptionNonce: 0,
      });

      let attempts = 0;
      const realCompareAndSwap = A.stateRepository.compareAndSwap.bind(
        A.stateRepository
      );
      A.stateRepository.compareAndSwap = (args) => {
        attempts += 1;
        const current = A.stateRepository.getByAggregateId(1);

        A.stateRepository.replace(
          attempts === 4
            ? truth
            : { ...current, corruptionNonce: attempts }
        );

        return realCompareAndSwap(args);
      };

      const returned = engine.getState(1, { consistency: "authoritative" });

      assert.equal(attempts, 4, "the initial write and all bounded retries must lose");
      assert.deepEqual(returned, truth);
      assert.deepEqual(A.stateRepository.getByAggregateId(1), truth);
    });
  });
}

// ===========================================================================
// MV-C10 - deliberate corruption injection must keep working unconditionally
// ===========================================================================
describe("Materialized view drift injection is unaffected", () => {
  for (const storeType of ["memory", "sqlite"]) {
    test(`MV-C10: unconditional replace still injects drift (${storeType})`, () => {
      const { A } = workers(storeType);
      const engine = engineOn(A, "worker-A");
      engine.checkout(PAYLOAD, { commandId: "c1" });
      const current = A.stateRepository.getByAggregateId(1);

      // Exactly what chaos VIEW_CORRUPT does: equal version, different state.
      A.stateRepository.replace({ ...current, order: { ...current.order, item: "CORRUPTED_CACHE_ITEM", quantity: 9999 } });
      assert.equal(A.stateRepository.getByAggregateId(1).order.item, "CORRUPTED_CACHE_ITEM");

      // Exactly what chaos VIEW_DELETE does.
      A.stateRepository.replace({ aggregateId: 1, deleted: true, version: current.version });
      assert.equal(A.stateRepository.getByAggregateId(1).deleted, true);
      close(A);
    });
  }
});
