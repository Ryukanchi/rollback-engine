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
    clock = () => new Date().toISOString(),
    diagnosticReporter,
    startingIds = {},
  } = {}) {
    assertFunction(eventIdGenerator, "eventIdGenerator");
    assertFunction(operationIdGenerator, "operationIdGenerator");
    assertFunction(clock, "clock");

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
      diagnosticReporter,
      emitDiagnostic: this.#emitDiagnostic,
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
          getState: () => this.#stateRepository.getByAggregateId(aggregateId),
        });

        const state = this.getLiveState(aggregateId);
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
          state: this.getLiveState(aggregateId),
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
        const currentState = this.#ensureLiveState(aggregateId);

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
        const state = this.getLiveState(aggregateId);
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
      const state = this.#ensureLiveState(aggregateId);
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
      return this.#ensureLiveState(aggregateId);
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
        const currentState = this.#ensureLiveState(aggregateId);

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
            getState: () => this.#stateRepository.getByAggregateId(aggregateId),
          }
        );

        const state = this.getLiveState(aggregateId);

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
    const state = useSnapshot
      ? this.replayFromSnapshot(aggregateId)
      : this.replay(aggregateId);

    if (!state) {
      return null;
    }

    return this.#writeMaterializedState(state);
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
    const replayedState = this.replay(aggregateId);

    if (!replayedState) {
      return null;
    }

    const currentState = this.#stateRepository.getByAggregateId(aggregateId);

    if (!currentState || !isDeepStrictEqual(currentState, replayedState)) {
      return this.#writeMaterializedState(replayedState);
    }

    return currentState;
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
    const currentState =
      this.#ensureLiveState(aggregateId) || createInitialState(aggregateId);

    const nextState = applyEvent(currentState, event);
    const storedEvent = this.#commandCoordinator.commitEvent(
      event,
      commandContext,
      lastSequence
    );

    try {
      this.#writeMaterializedState(nextState);
    } catch (updateError) {
      try {
        const repairedState = this.#writeMaterializedState(this.replay(aggregateId));

        if (!isDeepStrictEqual(repairedState, nextState)) {
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

  #writeMaterializedState(state) {
    const currentState = this.#stateRepository.getByAggregateId(state.aggregateId);

    if (currentState && isDeepStrictEqual(currentState, state)) {
      return currentState;
    }

    if (currentState) {
      this.#stateRepository.replace(state);
    } else {
      this.#stateRepository.save(state);
    }

    const storedState = this.#stateRepository.getByAggregateId(state.aggregateId);

    if (!isDeepStrictEqual(storedState, state)) {
      throw new Error(
        `Materialized view for aggregate ${state.aggregateId} does not match replay`
      );
    }

    return storedState;
  }
}

module.exports = {
  RollbackEngine,
};
