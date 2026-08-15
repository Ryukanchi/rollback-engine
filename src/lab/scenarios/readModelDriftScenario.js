const { checkInvariants } = require("../invariantChecker");

function runReadModelDriftScenario(session) {
  const commandId = `cmd-drift-${session.scenarioId}`;
  const checkoutPayload = {
    item: "PrecisionCalibrator",
    quantity: 1,
    amount: 950,
  };

  const checkoutResult = session.engine.checkout(checkoutPayload, { commandId });
  const aggregateId = checkoutResult.aggregateId;

  // Step 2: Inject deliberate logical drift into the materialized read model table in SQLite
  const corruptedState = {
    aggregateId,
    version: 3,
    lifecycle: "completed",
    order: { item: "MUTATED_STALE_CACHE", quantity: 999 },
    inventory: { reservationId: 1, status: "reserved", item: "MUTATED_STALE_CACHE", quantity: 999 },
    payment: { paymentId: 1, status: "charged", amount: 9999 },
  };

  if (session.storageType === "sqlite" && session.adapters.db) {
    session.adapters.db
      .prepare("UPDATE materialized_states SET state = ? WHERE aggregate_id = ?")
      .run(JSON.stringify(corruptedState), aggregateId);
  } else {
    session.adapters.stateRepository.replace(corruptedState);
  }

  // Save aggregateId on session for subsequent repair call
  session.extra.aggregateId = aggregateId;

  const events = session.engine.getEvents(aggregateId);
  const authoritativeState = session.engine.replay(aggregateId);
  const materializedState = session.engine.getState(aggregateId, { consistency: "materialized" });
  const diagnostics = session.engine.getDiagnostics({ aggregateId });

  const invariants = checkInvariants({
    events,
    authoritativeState,
    materializedState,
    replayedState: authoritativeState,
  });

  return {
    scenarioId: session.scenarioId,
    scenarioType: "read_model_drift",
    status: "drift_detected",
    aggregateId,
    commandId,
    storage: {
      adapter: session.storageType,
      persistent: session.storageType === "sqlite",
      databaseFile: session.dbFileName,
    },
    events,
    authoritativeState,
    materializedState,
    diagnostics,
    invariants,
    drift: {
      detected: true,
      authoritativeItem: authoritativeState.order?.item || "-",
      materializedItem: materializedState.order?.item || "-",
      canRepair: true,
    },
    summary: {
      headline: "Logical Read-Model Drift Injected",
      description: "Materialized cache row was deliberately corrupted. Authoritative event log remains untouched. Discrepancy detected between materialized view and authoritative replay.",
      eventsCommitted: events.length,
      lifecycle: authoritativeState.lifecycle,
    },
  };
}

function repairReadModelDrift(session) {
  const aggregateId = session.extra.aggregateId;
  if (!aggregateId) {
    throw new Error("No active drift scenario found for this session");
  }

  // Perform authoritative read which triggers automatic self-healing in RollbackEngine
  session.engine.getState(aggregateId, { consistency: "authoritative" });
  const authoritativeState = session.engine.replay(aggregateId);
  const repairedMaterializedState = session.engine.getState(aggregateId, { consistency: "materialized" });
  const events = session.engine.getEvents(aggregateId);
  const diagnostics = session.engine.getDiagnostics({ aggregateId });

  const invariants = checkInvariants({
    events,
    authoritativeState,
    materializedState: repairedMaterializedState,
    replayedState: authoritativeState,
  });

  return {
    scenarioId: session.scenarioId,
    scenarioType: "read_model_drift",
    status: "repaired",
    aggregateId,
    storage: {
      adapter: session.storageType,
      persistent: session.storageType === "sqlite",
      databaseFile: session.dbFileName,
    },
    events,
    authoritativeState,
    materializedState: repairedMaterializedState,
    diagnostics,
    invariants,
    drift: {
      detected: false,
      repaired: true,
      authoritativeItem: authoritativeState.order?.item || "-",
      materializedItem: repairedMaterializedState.order?.item || "-",
    },
    summary: {
      headline: "Self-Healing Completed",
      description: "Authoritative read detected cache drift, performed full replay from the append-only event store, and successfully repaired the materialized view table.",
      eventsCommitted: events.length,
      lifecycle: authoritativeState.lifecycle,
    },
  };
}

module.exports = {
  runReadModelDriftScenario,
  repairReadModelDrift,
};
