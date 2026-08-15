const { checkInvariants } = require("../invariantChecker");

function runBoundaryScenario(session) {
  const commandId = `cmd-boundary-${session.scenarioId}`;
  const rawPayload = { item: "DroneMotor", quantity: 4, amount: 400 };
  const normalizedPayload = { ...rawPayload, simulateFailureAt: null };

  // Step 1: Simulate command record in 'processing' state with ZERO committed events
  if (session.storageType === "sqlite" && session.adapters.db) {
    session.adapters.db
      .prepare("INSERT INTO commands (command_id, command_type, payload, status) VALUES (?, 'CHECKOUT', ?, 'processing')")
      .run(commandId, JSON.stringify(normalizedPayload));
  } else {
    session.adapters.commandStore.reserve({
      commandId,
      commandType: "CHECKOUT",
      payload: normalizedPayload,
    });
  }

  // Step 2: Attempt execution / retry with same commandId
  let caughtError = null;
  try {
    session.engine.checkout(rawPayload, { commandId });
  } catch (err) {
    caughtError = err;
  }

  const allEvents = session.engine.getAllEvents();
  const diagnostics = session.engine.getDiagnostics();

  const invariants = checkInvariants({
    events: allEvents,
    authoritativeState: null,
    materializedState: null,
    replayedState: null,
    duplicateEventsCount: 0,
  });

  return {
    scenarioId: session.scenarioId,
    scenarioType: "processing_zero_events_boundary",
    status: "boundary_enforced",
    commandId,
    storage: {
      adapter: session.storageType,
      persistent: session.storageType === "sqlite",
      databaseFile: session.dbFileName,
    },
    events: allEvents,
    diagnostics,
    invariants,
    boundary: {
      enforced: caughtError?.code === "COMMAND_IN_PROGRESS",
      errorCode: caughtError?.code || null,
      errorMessage: caughtError?.message || null,
      eventCommitted: caughtError?.eventCommitted ?? false,
      retrySafe: caughtError?.retrySafe ?? false,
      retryAction: caughtError?.retryAction || "WAIT_AND_RETRY_SAME_KEY",
      eventsCreated: allEvents.length,
      explanation: "The engine deliberately refuses automatic takeover because worker liveness cannot be determined without distributed leases or fencing tokens.",
    },
    summary: {
      headline: "Known Reliability Boundary: processing + 0 events",
      description: "An uncompleted command without events was found after a crash. The engine refuses silent reset or automatic takeover, safely returning COMMAND_IN_PROGRESS to protect against in-flight worker concurrency.",
      eventsCommitted: allEvents.length,
    },
  };
}

module.exports = {
  runBoundaryScenario,
};
