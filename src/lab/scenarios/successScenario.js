const { checkInvariants } = require("../invariantChecker");

function runSuccessfulCheckoutScenario(session) {
  const commandId = `cmd-success-${session.scenarioId}`;
  const checkoutPayload = {
    item: "ServerRack",
    quantity: 2,
    amount: 1500,
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
    scenarioType: "successful_checkout",
    status: "completed",
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
      headline: "Successful 3-Step Checkout Saga",
      description: "Standard checkout completed: order created, inventory reserved, payment charged. All 3 events committed and projected.",
      eventsCommitted: events.length,
      lifecycle: authoritativeState.lifecycle,
    },
  };
}

module.exports = {
  runSuccessfulCheckoutScenario,
};
