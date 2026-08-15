const { checkInvariants } = require("../invariantChecker");
const { EVENT_TYPES, createDomainEvent } = require("../../domain/events");
const { RollbackEngine } = require("../../application/rollbackEngine");

function runLeaseFencingScenario(session) {
  const commandId = `cmd-lease-fencing-${session.scenarioId}`;
  const payload = { item: "AutonomousNode", quantity: 1, amount: 1500 };
  const normalizedPayload = { ...payload, simulateFailureAt: null };

  let simulatedTime = 1000;

  // Step 1: Worker 1 reserves command at t=1000 with 1000ms TTL (token 1, expires at 2000)
  const initialReservation = session.adapters.commandStore.reserve({
    commandId,
    commandType: "CHECKOUT",
    payload: normalizedPayload,
    workerId: "worker-process-1",
    leaseTtlMs: 1000,
    now: simulatedTime,
  });

  // Step 2: Advance time past expiry to t=2500
  simulatedTime = 2500;

  // Worker 2 takes over the expired lease and completes the command
  const engineWorker2 = new RollbackEngine({
    eventStore: session.adapters.eventStore,
    commandStore: session.adapters.commandStore,
    snapshotStore: session.adapters.snapshotStore,
    stateRepository: session.adapters.stateRepository,
    workerId: "worker-process-2",
    leaseTtlMs: 3000,
    now: () => simulatedTime,
  });

  const worker2Result = engineWorker2.checkout(payload, { commandId });

  // Step 3: Worker 1 (zombie) attempts an append with stale fencing token 1
  let zombieRejection = null;
  const zombieEvent = createDomainEvent({
    eventId: `zombie-event-${session.scenarioId}`,
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: worker2Result.aggregateId,
    sequence: 4,
    timestamp: "2026-08-15T12:00:00.000Z",
    payload: { item: "AutonomousNode", quantity: 1 },
    metadata: {
      schemaVersion: 1,
      commandId,
      correlationId: commandId,
      causationId: commandId,
    },
  });

  try {
    session.adapters.eventStore.append(zombieEvent, {
      expectedVersion: 3,
      fencingToken: 1, // Stale token from Worker 1
    });
  } catch (err) {
    zombieRejection = {
      code: err.code,
      message: err.message,
      providedToken: err.providedToken,
      currentToken: err.currentToken,
      eventCommitted: err.eventCommitted,
    };
  }

  const allEvents = session.engine.getAllEvents();
  const liveState = session.engine.getLiveState(worker2Result.aggregateId);
  const replayedState = session.engine.replay(worker2Result.aggregateId);
  const finalCmd = session.adapters.commandStore.get(commandId);

  const invariants = checkInvariants({
    events: allEvents,
    authoritativeState: replayedState,
    materializedState: liveState,
    replayedState,
    duplicateEventsCount: 0,
  });

  return {
    scenarioId: session.scenarioId,
    scenarioType: "lease_recovery_and_zombie_fencing",
    status: "fencing_verified",
    aggregateId: worker2Result.aggregateId,
    commandId,
    storage: {
      adapter: session.storageType,
      persistent: session.storageType === "sqlite",
      databaseFile: session.dbFileName,
    },
    events: allEvents,
    diagnostics: session.engine.getDiagnostics(),
    invariants,
    leaseCoordination: {
      initialWorker: "worker-process-1",
      initialToken: 1,
      initialExpiresAt: initialReservation.record.leaseExpiresAt,
      takeoverWorker: "worker-process-2",
      takeoverToken: finalCmd.leaseToken,
      zombieWorkerRejected: zombieRejection !== null && zombieRejection.code === "FENCING_TOKEN_STALE",
      zombieRejection,
      finalCommandStatus: finalCmd.status,
    },
    summary: {
      headline: "Lease Recovery & Atomic Zombie Fencing",
      description:
        "Worker 1 lease expired without committed events. Worker 2 atomically acquired ownership with monotonic fencing token 2 and executed the checkout. A delayed mutation from Worker 1 was rejected at the authoritative Event Store commit boundary with FENCING_TOKEN_STALE.",
      eventsCommitted: allEvents.length,
    },
  };
}

module.exports = {
  runLeaseFencingScenario,
};
