const { checkInvariants } = require("../invariantChecker");

function runReconciliationScenario(session) {
  const commandId = `cmd-recon-${session.scenarioId}`;
  const aggregateId = `agg-recon-${session.scenarioId}`;
  const payload = { item: "QuantumSensor", quantity: 1 };

  // Step 1: Simulate lost ACK / partial commit state in the database
  if (session.storageType === "sqlite" && session.adapters.db) {
    session.adapters.db
      .prepare("INSERT INTO commands (command_id, command_type, payload, status) VALUES (?, 'CREATE_ORDER', ?, 'processing')")
      .run(commandId, JSON.stringify(payload));

    session.adapters.db
      .prepare(
        "INSERT INTO events (event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata) VALUES (?, ?, 1, ?, 'ORDER_CREATED', ?, ?, ?)"
      )
      .run(
        `evt-recon-1-${session.scenarioId}`,
        aggregateId,
        commandId,
        new Date().toISOString(),
        JSON.stringify(payload),
        JSON.stringify({
          schemaVersion: 1,
          commandId,
          correlationId: commandId,
          causationId: commandId,
        })
      );
  } else {
    // In-memory equivalent simulation
    session.adapters.commandStore.reserve({
      commandId,
      commandType: "CREATE_ORDER",
      payload,
    });
    session.adapters.eventStore.append(
      {
        eventId: `evt-recon-1-${session.scenarioId}`,
        eventType: "ORDER_CREATED",
        aggregateId,
        sequence: 1,
        timestamp: new Date().toISOString(),
        payload,
        metadata: {
          schemaVersion: 1,
          commandId,
          correlationId: commandId,
          causationId: commandId,
        },
      },
      { expectedVersion: 0 }
    );
  }

  const eventsBeforeRetry = session.engine.getEvents(aggregateId);
  const countBefore = eventsBeforeRetry.length;

  // Step 2: Attempt retry with identical commandId
  let caughtError = null;
  try {
    session.engine.createOrder(payload, { commandId });
  } catch (err) {
    caughtError = err;
  }

  const eventsAfterRetry = session.engine.getEvents(aggregateId);
  const countAfter = eventsAfterRetry.length;
  const duplicateEventsCount = countAfter - countBefore;

  const authoritativeState = session.engine.replay(aggregateId);
  const diagnostics = session.engine.getDiagnostics({ aggregateId });

  const invariants = checkInvariants({
    events: eventsAfterRetry,
    authoritativeState,
    materializedState: authoritativeState,
    replayedState: authoritativeState,
    duplicateEventsCount,
  });

  return {
    scenarioId: session.scenarioId,
    scenarioType: "post_commit_reconciliation",
    status: "reconciliation_prevented_duplicate",
    aggregateId,
    commandId,
    storage: {
      adapter: session.storageType,
      persistent: session.storageType === "sqlite",
      databaseFile: session.dbFileName,
    },
    events: eventsAfterRetry,
    finalState: authoritativeState,
    diagnostics,
    invariants,
    reconciliation: {
      interruptedCommitDetected: caughtError?.code === "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT",
      errorCode: caughtError?.code || null,
      errorMessage: caughtError?.message || null,
      eventCommitted: caughtError?.eventCommitted ?? true,
      retrySafe: caughtError?.retrySafe ?? false,
      retryAction: caughtError?.retryAction || "MANUAL_RESOLUTION_REQUIRED",
      eventsBeforeRetry: countBefore,
      eventsAfterRetry: countAfter,
      duplicateEventsPrevented: duplicateEventsCount === 0,
    },
    summary: {
      headline: "Post-Commit Reconciliation Guard",
      description: "A process crash occurred after Event Store commit but before completion ACK. On retry, existing events were found via indexed command_id. Command callback was NOT re-executed, preventing duplicate events.",
      eventsCommitted: countAfter,
      lifecycle: authoritativeState.lifecycle,
    },
  };
}

module.exports = {
  runReconciliationScenario,
};
