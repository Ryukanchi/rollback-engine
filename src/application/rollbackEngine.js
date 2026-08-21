const { randomUUID } = require("node:crypto");
const { isDeepStrictEqual } = require("node:util");

const {
  CURRENT_EVENT_SCHEMA_VERSION,
  EVENT_TYPES,
  createDomainEvent,
} = require("../domain/events");
const {
  compensateCheckout,
  runCheckoutSaga,
  validateCheckoutCommand,
} = require("../domain/checkoutSaga");
const {
  applyEvent,
  createInitialState,
  projectEvents,
} = require("../domain/projection");
const {
  CommandExecutionCoordinator,
} = require("./commandExecutionCoordinator");
const {
  assertEventStoreAdapter,
  assertSnapshotStoreAdapter,
  assertStateRepositoryAdapter,
} = require("./storeContracts");
const {
  DIAGNOSTIC_STATUSES,
  DIAGNOSTIC_TYPES,
  createDiagnosticEmitter,
} = require("./diagnostics");
const { buildEventTimeline } = require("./eventTimeline");
const { InMemoryEventStore } = require("../infrastructure/inMemoryEventStore");
const {
  InMemoryCommandStore,
} = require("../infrastructure/inMemoryCommandStore");
const {
  InMemorySnapshotStore,
} = require("../infrastructure/inMemorySnapshotStore");
const {
  InMemoryStateRepository,
} = require("../infrastructure/inMemoryStateRepository");

/**
 * How many times a losing writer re-observes and retries before giving up.
 * Small on purpose: this is a convergence aid, not a mutual-exclusion device,
 * and exhaustion is surfaced rather than promoting an unvalidated view.
 */
const MATERIALIZED_VIEW_RECONCILE_ATTEMPTS = 3;

function assertFunction(value, name) {
  if (typeof value !== "function") {
    throw new TypeError(`${name} must be a function`);
  }
}

function assertPositiveInteger(value, name) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}

function assertNonNegativeInteger(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
}

function normalizeTimestamp(timestamp) {
  if (typeof timestamp !== "string" || timestamp.length === 0) {
    throw new TypeError("timestamp must be a valid date string");
  }

  const parsedTimestamp = new Date(timestamp);

  if (Number.isNaN(parsedTimestamp.getTime())) {
    throw new TypeError("timestamp must be a valid date string");
  }

  return parsedTimestamp.toISOString();
}

function validateOrderCommand(command) {
  if (!command || typeof command !== "object" || Array.isArray(command)) {
    throw new TypeError("order command must be an object");
  }

  if (typeof command.item !== "string" || command.item.trim().length === 0) {
    throw new TypeError("item must be a non-empty string");
  }

  if (!Number.isSafeInteger(command.quantity) || command.quantity <= 0) {
    throw new TypeError("quantity must be a positive safe integer");
  }
}

function createAggregateNotFoundError(aggregateId) {
  const error = new Error(`Aggregate ${aggregateId} does not exist`);
  error.code = "AGGREGATE_NOT_FOUND";
  return error;
}

function createCompensationRequiredError(aggregateId) {
  const error = new Error(
    `Aggregate ${aggregateId} must be compensated before it can be deleted`
  );
  error.code = "COMPENSATION_REQUIRED";
  return error;
}

function createMaterializedViewReconciliationError(aggregateId) {
  const error = new Error(
    `Materialized view reconciliation for aggregate ${aggregateId} exhausted without establishing an authoritative state`
  );
  error.code = "MATERIALIZED_VIEW_RECONCILIATION_EXHAUSTED";
  error.aggregateId = aggregateId;
  error.attempts = MATERIALIZED_VIEW_RECONCILE_ATTEMPTS;
  return error;
}

function createCommandResultAuthorityError(aggregateId) {
  const error = new Error(
    `Command result state for aggregate ${aggregateId} does not match authoritative replay`
  );
  error.code = "COMMAND_RESULT_STATE_NOT_AUTHORITATIVE";
  error.aggregateId = aggregateId;
  return error;
}

function createMaterializedViewUpdateError(
  aggregateId,
  event,
  updateError,
  recoveryError
) {
  const error = new Error(
    `Event ${event.eventId} was committed, but the materialized view for aggregate ${aggregateId} could not be repaired`,
    { cause: updateError }
  );
  error.code = "EVENT_COMMITTED_VIEW_REPAIR_FAILED";
  error.aggregateId = aggregateId;
  error.eventId = event.eventId;
  error.eventCommitted = true;
  error.retrySafe = false;
  error.retryAction = "MANUAL_RESOLUTION_REQUIRED";
  error.recoveryError = recoveryError;
  return error;
}

function createSnapshotWarning(aggregateId) {
  return {
    code: "SNAPSHOT_SAVE_FAILED",
    category: "technical",
    message:
      "The command committed successfully, but its snapshot could not be saved.",
    eventCommitted: true,
    retrySafe: false,
    retryAction: "DO_NOT_RETRY_COMMAND",
    aggregateId,
  };
}

class RollbackEngine {
  #eventStore;

  #stateRepository;

  #snapshotStore;

  #commandCoordinator;

  #eventIdGenerator;

  #clock;

  #emitDiagnostic;

  #nextAggregateId;

  #nextReservationId;

  #nextPaymentId;

  constructor({
    eventStore = new InMemoryEventStore(),
    stateRepository = new InMemoryStateRepository(),
    snapshotStore = new InMemorySnapshotStore(),
    commandStore = new InMemoryCommandStore(),
    eventIdGenerator = randomUUID,
    operationIdGenerator = randomUUID,
    workerId,
    leaseTtlMs = 30000,
    clock = () => new Date().toISOString(),
    now = null,
    diagnosticReporter,
    startingIds = {},
  } = {}) {
    assertFunction(eventIdGenerator, "eventIdGenerator");
    assertFunction(operationIdGenerator, "operationIdGenerator");
    assertFunction(clock, "clock");

    if (eventStore && typeof eventStore.setCommandStore === "function" && commandStore) {
      eventStore.setCommandStore(commandStore);
    }
    if (commandStore && typeof commandStore.setEventStore === "function" && eventStore) {
      commandStore.setEventStore(eventStore);
    }

    this.#eventStore = assertEventStoreAdapter(eventStore);
    this.#stateRepository = assertStateRepositoryAdapter(stateRepository);
    this.#snapshotStore = assertSnapshotStoreAdapter(snapshotStore);
    this.#eventIdGenerator = eventIdGenerator;
    this.#clock = clock;
    this.#emitDiagnostic = createDiagnosticEmitter(diagnosticReporter);
    this.#commandCoordinator = new CommandExecutionCoordinator({
      eventStore,
      commandStore,
      operationIdGenerator,
      workerId,
      leaseTtlMs,
      clock,
      now,
      diagnosticReporter,
      emitDiagnostic: this.#emitDiagnostic,
      validateResult: (result) => this.#validateCommandResult(result),
    });
    this.#nextAggregateId = startingIds.aggregateId ?? 1;
    this.#nextReservationId = startingIds.reservationId ?? 1;
    this.#nextPaymentId = startingIds.paymentId ?? 1;

    assertPositiveInteger(this.#nextAggregateId, "startingIds.aggregateId");
    assertPositiveInteger(this.#nextReservationId, "startingIds.reservationId");
    assertPositiveInteger(this.#nextPaymentId, "startingIds.paymentId");
  }

  checkout(command, options = {}) {
    validateCheckoutCommand(command);
    const normalizedCommand = {
      item: command.item,
      quantity: command.quantity,
      amount: command.amount,
      simulateFailureAt: command.simulateFailureAt ?? null,
    };

    return this.#commandCoordinator.execute(
      "CHECKOUT",
      normalizedCommand,
      options,
      (commandContext) => {
        const aggregateId = this.#allocateAggregateId();
        const context = {
          ...normalizedCommand,
          aggregateId,
          reservationId: this.#nextReservationId++,
          paymentId: this.#nextPaymentId++,
        };
        const sagaResult = runCheckoutSaga(context, {
          recordEvent: (eventType, payload) =>
            this.#recordEvent(
              aggregateId,
              eventType,
              payload,
              commandContext
            ),
          getState: () => this.#getAuthoritativeState(aggregateId),
        });

        const state = this.#getAuthoritativeState(aggregateId);
        const { snapshot, warnings } = this.#createSnapshotBestEffort(
          aggregateId,
          commandContext.eventCommandId
        );

        return {
          aggregateId,
          status: sagaResult.status,
          failedAt: sagaResult.failedAt,
          error: sagaResult.error,
          completedSteps: [...sagaResult.completedSteps],
          events: [...sagaResult.events],
          state,
          snapshot,
          warnings,
        };
      }
    );
  }

  createOrder({ item, quantity = 1 } = {}, options = {}) {
    const command = { item, quantity };
    validateOrderCommand(command);

    return this.#commandCoordinator.execute(
      "CREATE_ORDER",
      command,
      options,
      (commandContext) => {
        const aggregateId = this.#allocateAggregateId();
        const event = this.#recordEvent(
          aggregateId,
          EVENT_TYPES.ORDER_CREATED,
          command,
          commandContext
        );

        return {
          aggregateId,
          event,
          state: this.#getAuthoritativeState(aggregateId),
        };
      }
    );
  }

  deleteOrder(aggregateId, reason = "Order deleted", options = {}) {
    if (typeof reason !== "string" || reason.trim().length === 0) {
      throw new TypeError("reason must be a non-empty string");
    }

    return this.#commandCoordinator.execute(
      "DELETE_ORDER",
      { aggregateId, reason },
      options,
      (commandContext) => {
        const currentState = this.#getAuthoritativeState(aggregateId);

        if (!currentState || currentState.deleted || !currentState.order) {
          throw createAggregateNotFoundError(aggregateId);
        }

        if (
          currentState.payment?.status === "charged" ||
          currentState.inventory?.status === "reserved"
        ) {
          throw createCompensationRequiredError(aggregateId);
        }

        const deletedOrder = currentState.order;
        const event = this.#recordEvent(
          aggregateId,
          EVENT_TYPES.ORDER_DELETED,
          { reason },
          commandContext
        );
        const state = this.#getAuthoritativeState(aggregateId);
        const { snapshot, warnings } = this.#createSnapshotBestEffort(
          aggregateId,
          commandContext.eventCommandId
        );

        return {
          aggregateId,
          event,
          deletedOrder,
          state,
          snapshot,
          warnings,
        };
      }
    );
  }

  listOrders() {
    return this.#stateRepository
      .getAll()
      .filter((state) => !state.deleted && state.order)
      .map((state) => state.order);
  }

  getOrder(aggregateId, { consistency = "materialized" } = {}) {
    if (consistency === "authoritative") {
      const state = this.#getAuthoritativeState(aggregateId);
      if (!state || state.deleted || !state.order) {
        return null;
      }
      return state.order;
    }

    if (consistency === "materialized") {
      const state = this.#stateRepository.getByAggregateId(aggregateId);
      if (!state || state.deleted || !state.order) {
        return null;
      }
      return state.order;
    }

    throw new TypeError(`Unsupported consistency level: ${consistency}`);
  }

  getState(aggregateId, { consistency = "materialized" } = {}) {
    if (consistency === "authoritative") {
      return this.#getAuthoritativeState(aggregateId);
    }

    if (consistency === "materialized") {
      return this.#stateRepository.getByAggregateId(aggregateId);
    }

    throw new TypeError(`Unsupported consistency level: ${consistency}`);
  }

  compensate(
    aggregateId,
    reason = "Manual checkout compensation",
    options = {}
  ) {
    return this.#commandCoordinator.execute(
      "COMPENSATE_CHECKOUT",
      { aggregateId, reason },
      options,
      (commandContext) => {
        const currentState = this.#getAuthoritativeState(aggregateId);

        if (!currentState) {
          throw createAggregateNotFoundError(aggregateId);
        }

        const events = compensateCheckout(
          { reason },
          {
            recordEvent: (eventType, payload) =>
              this.#recordEvent(
                aggregateId,
                eventType,
                payload,
                commandContext
              ),
            getState: () => this.#getAuthoritativeState(aggregateId),
          }
        );

        const state = this.#getAuthoritativeState(aggregateId);

        return {
          aggregateId,
          status: state.lifecycle,
          events,
          state,
        };
      }
    );
  }

  replay(aggregateId) {
    return projectEvents(this.#eventStore.getByAggregateId(aggregateId));
  }

  replayFromSnapshot(aggregateId) {
    const aggregateEvents = this.#eventStore.getByAggregateId(aggregateId);

    if (aggregateEvents.length === 0) {
      return null;
    }

    const fullReplay = () => projectEvents(aggregateEvents);
    let snapshot;

    try {
      snapshot = this.#snapshotStore.getByAggregateId(aggregateId);
    } catch {
      return fullReplay();
    }

    if (!snapshot) {
      return fullReplay();
    }

    if (!this.#snapshotMatchesEventPrefix(snapshot, aggregateId, aggregateEvents)) {
      this.#emitDiagnostic({
        type: DIAGNOSTIC_TYPES.SNAPSHOT_FALLBACK_REPLAY,
        status: DIAGNOSTIC_STATUSES.FALLBACK_TO_FULL_REPLAY,
        aggregateId,
      });
      return fullReplay();
    }

    let state = structuredClone(snapshot.state);
    const eventsAfterSnapshot = aggregateEvents.slice(snapshot.version);

    for (const event of eventsAfterSnapshot) {
      state = applyEvent(state, event);
    }

    return state;
  }

  replayAt(aggregateId, timestamp) {
    const targetTimestamp = normalizeTimestamp(timestamp);
    const targetTime = new Date(targetTimestamp).getTime();
    const aggregateEvents = this.#eventStore.getByAggregateId(aggregateId);

    if (aggregateEvents.length === 0) {
      return null;
    }

    const eventsAtTimestamp = [];

    for (const event of aggregateEvents) {
      if (new Date(event.timestamp).getTime() > targetTime) {
        break;
      }

      eventsAtTimestamp.push(event);
    }

    return eventsAtTimestamp.length > 0
      ? projectEvents(eventsAtTimestamp)
      : createInitialState(aggregateId);
  }

  replayAtSequence(aggregateId, targetSequence) {
    assertNonNegativeInteger(targetSequence, "targetSequence");
    const aggregateEvents = this.#eventStore.getByAggregateId(aggregateId);

    if (aggregateEvents.length === 0) {
      return null;
    }

    if (targetSequence === 0) {
      return createInitialState(aggregateId);
    }

    const eventsUpToSequence = aggregateEvents.filter(
      (event) => event.sequence <= targetSequence
    );

    return eventsUpToSequence.length > 0
      ? projectEvents(eventsUpToSequence)
      : createInitialState(aggregateId);
  }

  createSnapshot(aggregateId) {
    const state = this.replay(aggregateId);

    if (!state) {
      throw createAggregateNotFoundError(aggregateId);
    }

    const aggregateEvents = this.#eventStore.getByAggregateId(aggregateId);
    const lastEvent = aggregateEvents[state.version - 1];

    return this.#snapshotStore.save({
      aggregateId,
      version: state.version,
      timestamp: normalizeTimestamp(this.#clock()),
      lastEventId: lastEvent?.eventId,
      state,
    });
  }

  getSnapshot(aggregateId) {
    return this.#snapshotStore.getByAggregateId(aggregateId);
  }

  recover(aggregateId, { useSnapshot = true } = {}) {
    const observedState = this.#stateRepository.getByAggregateId(aggregateId);
    const state = useSnapshot
      ? this.replayFromSnapshot(aggregateId)
      : this.replay(aggregateId);

    if (!state) {
      return null;
    }

    return this.#writeMaterializedState(observedState, state);
  }

  getLiveState(aggregateId) {
    return this.#stateRepository.getByAggregateId(aggregateId);
  }

  getEvents(aggregateId) {
    return this.#eventStore.getByAggregateId(aggregateId);
  }

  getAllEvents() {
    return this.#eventStore.getAll();
  }

  getTimeline(aggregateId) {
    return buildEventTimeline(this.#eventStore.getByAggregateId(aggregateId));
  }

  getDiagnostics(filter = {}) {
    if (typeof this.#emitDiagnostic?.query === "function") {
      return this.#emitDiagnostic.query(filter);
    }
    return [];
  }

  subscribe(filter, handler) {
    if (typeof this.#eventStore.subscribe === "function") {
      return this.#eventStore.subscribe(filter, handler);
    }
    return () => {};
  }

  #allocateAggregateId() {
    while (this.#eventStore.getLastSequence(this.#nextAggregateId) > 0) {
      this.#nextAggregateId += 1;
    }

    return this.#nextAggregateId++;
  }

  #ensureLiveState(aggregateId) {
    // Observed before the replay on purpose: this is the state the repair is
    // conditioned on, so anything that changes the view afterwards has to make
    // the write lose rather than be silently overwritten.
    const currentState = this.#stateRepository.getByAggregateId(aggregateId);
    const replayedState = this.replay(aggregateId);

    if (!replayedState) {
      return null;
    }

    if (!currentState || !isDeepStrictEqual(currentState, replayedState)) {
      return this.#writeMaterializedState(currentState, replayedState);
    }

    return currentState;
  }

  #getAuthoritativeState(aggregateId) {
    // #ensureLiveState may observe the view for conditional repair, but it can
    // return it only after full replay proves semantic equality. Saga and
    // command code use this named boundary so a materialized-only read cannot
    // accidentally acquire domain authority again.
    return this.#ensureLiveState(aggregateId);
  }

  #validateCommandResult(result) {
    if (
      !result ||
      typeof result !== "object" ||
      Array.isArray(result) ||
      !Object.prototype.hasOwnProperty.call(result, "state")
    ) {
      throw createCommandResultAuthorityError(result?.aggregateId);
    }

    const authoritativeState = this.#getAuthoritativeState(result.aggregateId);

    if (!isDeepStrictEqual(result.state, authoritativeState)) {
      throw createCommandResultAuthorityError(result.aggregateId);
    }

    if (authoritativeState === null) {
      return null;
    }

    const sequence = authoritativeState.version;
    const lastEvent =
      sequence === 0
        ? null
        : this.#eventStore
            .getByAggregateId(authoritativeState.aggregateId)
            .find((event) => event.sequence === sequence);

    if (sequence > 0 && !lastEvent) {
      throw createCommandResultAuthorityError(result.aggregateId);
    }

    return {
      aggregateId: authoritativeState.aggregateId,
      sequence,
      lastEventId: lastEvent?.eventId ?? null,
    };
  }

  #recordEvent(aggregateId, eventType, payload, commandContext) {
    const lastSequence = this.#eventStore.getLastSequence(aggregateId);
    const sequence = lastSequence + 1;
    const event = createDomainEvent({
      eventId: this.#eventIdGenerator(),
      eventType,
      aggregateId,
      sequence,
      timestamp: this.#clock(),
      payload,
      metadata: {
        schemaVersion: CURRENT_EVENT_SCHEMA_VERSION,
        commandId: commandContext.eventCommandId,
        correlationId: commandContext.correlationId,
        causationId:
          commandContext.lastEventId ?? commandContext.initialCausationId,
      },
    });
    const observedState = this.#getAuthoritativeState(aggregateId);
    const currentState = observedState || createInitialState(aggregateId);

    const nextState = applyEvent(currentState, event);
    const storedEvent = this.#commandCoordinator.commitEvent(
      event,
      commandContext,
      lastSequence
    );

    try {
      this.#writeMaterializedState(observedState, nextState);
    } catch (updateError) {
      try {
        const repairedState = this.#reconcileMaterializedState(aggregateId);

        if (!isDeepStrictEqual(repairedState, this.replay(aggregateId))) {
          throw new Error(
            `Repaired materialized view does not match replay for aggregate ${aggregateId}`
          );
        }

        this.#emitDiagnostic({
          type: DIAGNOSTIC_TYPES.MATERIALIZED_VIEW_REPAIR,
          status: DIAGNOSTIC_STATUSES.REPAIRED,
          commandId: storedEvent.metadata.commandId,
          aggregateId,
          eventId: storedEvent.eventId,
        });
      } catch (recoveryError) {
        this.#emitDiagnostic({
          type: DIAGNOSTIC_TYPES.MATERIALIZED_VIEW_REPAIR,
          status: DIAGNOSTIC_STATUSES.REPAIR_FAILED,
          commandId: storedEvent.metadata.commandId,
          aggregateId,
          eventId: storedEvent.eventId,
        });
        throw createMaterializedViewUpdateError(
          aggregateId,
          storedEvent,
          updateError,
          recoveryError
        );
      }
    }

    return storedEvent;
  }

  #createSnapshotBestEffort(aggregateId, commandId) {
    try {
      return {
        snapshot: this.createSnapshot(aggregateId),
        warnings: [],
      };
    } catch {
      this.#emitDiagnostic({
        type: DIAGNOSTIC_TYPES.SNAPSHOT_SAVE,
        status: DIAGNOSTIC_STATUSES.SAVE_FAILED,
        commandId,
        aggregateId,
      });
      return {
        snapshot: null,
        warnings: [createSnapshotWarning(aggregateId)],
      };
    }
  }

  #snapshotMatchesEventPrefix(snapshot, aggregateId, aggregateEvents) {
    if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) {
      return false;
    }

    if (snapshot.aggregateId !== aggregateId) {
      return false;
    }

    if (
      !Number.isSafeInteger(snapshot.version) ||
      snapshot.version <= 0 ||
      snapshot.version > aggregateEvents.length
    ) {
      return false;
    }

    try {
      if (normalizeTimestamp(snapshot.timestamp) !== snapshot.timestamp) {
        return false;
      }

      const expectedState = projectEvents(
        aggregateEvents.slice(0, snapshot.version)
      );

      return isDeepStrictEqual(snapshot.state, expectedState);
    } catch {
      return false;
    }
  }

  /**
   * The one place the materialized view is written, by publication and repair
   * alike - they are the same operation with different sources for `nextState`.
   *
   * Every write is conditional on the state the writer actually observed, so a
   * writer that fell behind loses instead of overwriting. The condition is that
   * observed state's identity and never its version, which is what keeps
   * authoritative repair free to move the view backwards (v8 corrupt -> v3).
   */
  #writeMaterializedState(expectedState, nextState) {
    if (expectedState && isDeepStrictEqual(expectedState, nextState)) {
      return expectedState;
    }

    const outcome = this.#stateRepository.compareAndSwap({
      aggregateId: nextState.aggregateId,
      expectedState: expectedState ?? null,
      nextState,
    });

    if (outcome.applied) {
      return nextState;
    }

    // The view moved after we looked. The candidate is stale by definition and
    // must never be forced through; converge on replay instead.
    return this.#reconcileMaterializedState(nextState.aggregateId);
  }

  /**
   * Re-observe, replay, and try again - bounded. Each attempt either wins or
   * proves another writer got there first. Under sustained concurrency the
   * caller fails closed: a losing CAS says nothing about whether the winning
   * materialized state is semantically correct.
   */
  #reconcileMaterializedState(aggregateId) {
    for (let attempt = 0; attempt < MATERIALIZED_VIEW_RECONCILE_ATTEMPTS; attempt += 1) {
      const observedState = this.#stateRepository.getByAggregateId(aggregateId);
      const replayedState = this.replay(aggregateId);

      if (isDeepStrictEqual(observedState, replayedState)) {
        return observedState;
      }

      if (!replayedState) {
        throw createMaterializedViewReconciliationError(aggregateId);
      }

      const outcome = this.#stateRepository.compareAndSwap({
        aggregateId,
        expectedState: observedState ?? null,
        nextState: replayedState,
      });

      if (outcome.applied) {
        return replayedState;
      }
    }

    // The final CAS may have lost to a correct writer. Re-observe once so that
    // case can still succeed, but only after validating it against fresh replay.
    const observedState = this.#stateRepository.getByAggregateId(aggregateId);
    const replayedState = this.replay(aggregateId);

    if (isDeepStrictEqual(observedState, replayedState)) {
      return observedState;
    }

    throw createMaterializedViewReconciliationError(aggregateId);
  }
}

module.exports = {
  RollbackEngine,
};
