const { checkInvariants } = require("../invariantChecker");

function runCompensationScenario(session) {
  const commandId = `cmd-comp-${session.scenarioId}`;
  const checkoutPayload = {
    item: "HighEndGPU",
    quantity: 1,
    amount: 1200,
    simulateFailureAt: "after_payment",
  };

  const checkoutResult = session.engine.checkout(checkoutPayload, { commandId });
  const aggregateId = checkoutResult.aggregateId;

  const events = session.engine.getEvents(aggregateId);
  const authoritativeState = session.engine.replay(aggregateId);
  const replayedState = session.engine.replay(aggregateId);
  const materializedState = session.engine.getState(aggregateId, { consistency: "materialized" });
  const snapshot = session.engine.getSnapshot(aggregateId);
  const diagnostics = session.engine.getDiagnostics({ aggregateId });

  const invariants = checkInvariants({
    events,
    authoritativeState,
    materializedState,
    replayedState,
  });

  return {
    scenarioId: session.scenarioId,
    scenarioType: "compensation_after_payment",
    status: "rolled_back",
    aggregateId,
    commandId,
    storage: {
      adapter: session.storageType,
      persistent: session.storageType === "sqlite",
      databaseFile: session.dbFileName,
    },
    events,
    finalState: authoritativeState,
    materializedState,
    snapshot,
    diagnostics,
    invariants,
    summary: {
      headline: "Saga Fault Injection & Reverse Compensation",
      description: "Fault injected after payment step. Forward steps (1-3) followed by deterministic reverse compensation steps (4-6). Final state is safely rolled back.",
      eventsCommitted: events.length,
      lifecycle: authoritativeState.lifecycle,
      failurePoint: "after_payment",
      forwardEventsCount: 3,
      compensationEventsCount: 3,
    },
  };
}

module.exports = {
  runCompensationScenario,
};
