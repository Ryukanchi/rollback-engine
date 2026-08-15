const { createStorageAdapters } = require("../../infrastructure/storageFactory");
const { RollbackEngine } = require("../../application/rollbackEngine");
const { ExecutionTrace } = require("../executionTrace");
const { classifyOutcome } = require("../failureClassifier");

class MemoryProfileRunner {
  static runIteration({ seed, iteration, prng, generator, invariantSuite }) {
    const trace = new ExecutionTrace({ seed, iteration, profile: "memory" });
    const adapters = createStorageAdapters({ type: "memory" });
    const engine = new RollbackEngine({
      eventStore: adapters.eventStore,
      commandStore: adapters.commandStore,
      snapshotStore: adapters.snapshotStore,
      stateRepository: adapters.stateRepository,
    });

    const context = {
      knownAggregates: [],
      knownCommands: [],
      driftedAggregates: new Set(),
      isFaultInjected: false,
      supportsSnapshotDelete: false,
    };

    const stats = {
      operations: 0,
      domainRejections: 0,
      failuresInjected: 0,
      retries: 0,
      recoveries: 0,
    };

    const numOps = prng.nextInt(4, 10);

    for (let step = 0; step < numOps; step++) {
      const op = generator.generateOperation(context);
      stats.operations++;

      let outcome = { success: false, result: null, error: null };
      let invariantsChecked = [];

      try {
        outcome = executeOperation({ op, engine, adapters, context, stats, invariantSuite });
      } catch (err) {
        outcome = { success: false, result: null, error: err };
      }

      const classification = classifyOutcome({
        success: outcome.success,
        error: outcome.error,
        context,
      });

      if (classification === "EXPECTED_DOMAIN_REJECTION") {
        stats.domainRejections++;
      } else if (classification === "EXPECTED_FAULT_INJECTION") {
        stats.failuresInjected++;
      }

      // Check invariants after this step
      try {
        invariantsChecked = invariantSuite.checkAll({
          engine,
          adapters,
          aggregateIds: context.knownAggregates,
          context,
        });
      } catch (invErr) {
        trace.recordStep({
          opName: op.type,
          args: op.params,
          outcome,
          classification: "INVARIANT_VIOLATION",
          invariantsChecked,
        });
        return {
          success: false,
          violatedInvariant: invErr.invariantName || "UnknownInvariant",
          error: invErr,
          trace,
          engine,
          adapters,
          affectedAggregateId: op.params?.aggregateId || context.knownAggregates[0],
          stats,
        };
      }

      trace.recordStep({
        opName: op.type,
        args: op.params,
        outcome,
        classification,
        invariantsChecked,
      });
    }

    return {
      success: true,
      trace,
      stats,
    };
  }
}

function executeOperation({ op, engine, adapters, context, stats, invariantSuite }) {
  switch (op.type) {
    case "CHECKOUT": {
      const { payload, options } = op.params;
      if (payload.simulateFailureAt) {
        context.isFaultInjected = true;
        stats.failuresInjected++;
        if (invariantSuite) invariantSuite.recordSagaFailure(payload.simulateFailureAt);
      }
      const res = engine.checkout(payload, options);
      if (!context.knownAggregates.includes(res.aggregateId)) {
        context.knownAggregates.push(res.aggregateId);
      }
      context.knownCommands.push({
        commandId: options.commandId,
        commandType: "CHECKOUT",
        payload,
      });
      return { success: true, result: res };
    }

    case "CREATE_ORDER": {
      const { command, options } = op.params;
      const res = engine.createOrder(command, options);
      if (!context.knownAggregates.includes(res.aggregateId)) {
        context.knownAggregates.push(res.aggregateId);
      }
      context.knownCommands.push({
        commandId: options.commandId,
        commandType: "CREATE_ORDER",
        payload: command,
      });
      return { success: true, result: res };
    }

    case "DELETE_ORDER": {
      const { aggregateId, reason, options } = op.params;
      const res = engine.deleteOrder(aggregateId, reason, options);
      return { success: true, result: res };
    }

    case "COMPENSATE": {
      const { aggregateId, reason, options } = op.params;
      const res = engine.compensate(aggregateId, reason, options);
      return { success: true, result: res };
    }

    case "RETRY_SAME_COMMAND": {
      stats.retries++;
      if (invariantSuite) invariantSuite.recordIdempotentRetryCheck();
      const { commandId, commandType, payload } = op.params;
      let res;
      if (commandType === "CHECKOUT") {
        res = engine.checkout(payload, { commandId });
      } else {
        res = engine.createOrder(payload, { commandId });
      }
      return { success: true, result: res };
    }

    case "RETRY_CONFLICTING_COMMAND": {
      stats.retries++;
      if (invariantSuite) invariantSuite.recordIdempotencyConflictCheck();
      const { commandId, commandType, payload } = op.params;
      let res;
      if (commandType === "CHECKOUT") {
        res = engine.checkout(payload, { commandId });
      } else {
        res = engine.createOrder(payload, { commandId });
      }
      return { success: true, result: res };
    }

    case "AUTHORITATIVE_READ": {
      const res = engine.getState(op.params.aggregateId, { consistency: "authoritative" });
      context.driftedAggregates.delete(op.params.aggregateId); // Authoritative read self-heals view!
      return { success: true, result: res };
    }

    case "MATERIALIZED_READ": {
      const res = engine.getState(op.params.aggregateId, { consistency: "materialized" });
      return { success: true, result: res };
    }

    case "RECOVER": {
      stats.recoveries++;
      const res = engine.recover(op.params.aggregateId);
      context.driftedAggregates.delete(op.params.aggregateId); // Recover rebuilds view!
      return { success: true, result: res };
    }

    case "SNAPSHOT_CREATE": {
      const res = engine.createSnapshot(op.params.aggregateId);
      return { success: true, result: res };
    }

    case "REPLAY": {
      const res = engine.replay(op.params.aggregateId);
      return { success: true, result: res };
    }

    case "REPLAY_AT_SEQUENCE": {
      const res = engine.replayAtSequence(op.params.aggregateId, op.params.targetSequence);
      return { success: true, result: res };
    }

    case "VIEW_CORRUPT": {
      const { aggregateId } = op.params;
      const current = engine.getState(aggregateId, { consistency: "materialized" });
      if (current) {
        const corrupted = {
          ...current,
          order: { ...current.order, item: "CORRUPTED_CACHE_ITEM", quantity: 9999 },
        };
        adapters.stateRepository.replace(corrupted);
        context.driftedAggregates.add(aggregateId);
      }
      return { success: true, result: { corrupted: true } };
    }

    case "VIEW_DELETE": {
      const { aggregateId } = op.params;
      const current = adapters.stateRepository.getByAggregateId(aggregateId);
      if (current) {
        adapters.stateRepository.replace({ aggregateId, deleted: true, version: current.version });
        context.driftedAggregates.add(aggregateId);
      }
      return { success: true, result: { viewDeleted: true } };
    }

    case "SNAPSHOT_CORRUPT": {
      const { aggregateId } = op.params;
      const snap = adapters.snapshotStore.getByAggregateId(aggregateId);
      if (snap) {
        adapters.snapshotStore.save({
          ...snap,
          version: snap.version + 999, // Ahead version triggers fallback
        });
      }
      return { success: true, result: { snapshotCorrupted: true } };
    }

    case "SNAPSHOT_DELETE": {
      return { success: true, result: { skipped: true } };
    }

    case "CONCURRENT_APPEND_ATTEMPT": {
      if (invariantSuite) invariantSuite.recordCommitBoundaryCheck();
      const { aggregateId, staleExpectedVersion } = op.params;
      try {
        adapters.eventStore.append(
          aggregateId,
          {
            eventId: `evt-stale-${Date.now()}`,
            eventType: "ORDER_CREATED",
            payload: { item: "StaleItem", quantity: 1 },
          },
          { expectedVersion: staleExpectedVersion }
        );
        return { success: true };
      } catch (err) {
        return { success: false, error: err };
      }
    }

    case "INTERRUPTED_COMMIT_SIMULATION": {
      if (invariantSuite) invariantSuite.recordPostCommitSafetyCheck();
      const { commandId } = op.params;
      const aggId = context.knownAggregates[0] ?? 99;
      // Reserve command
      adapters.commandStore.reserve({
        commandId,
        commandType: "CHECKOUT",
        payload: { item: "LostAckItem", quantity: 1, amount: 100, simulateFailureAt: null },
      });
      // Append event directly with commandId
      adapters.eventStore.append(
        aggId,
        {
          eventId: `evt-lost-${Date.now()}`,
          eventType: "ORDER_CREATED",
          payload: { item: "LostAckItem", quantity: 1 },
          metadata: { commandId },
        },
        { expectedVersion: 0 }
      );
      if (!context.knownAggregates.includes(aggId)) {
        context.knownAggregates.push(aggId);
      }
      // Retry should trigger reconciliation rejection
      try {
        engine.checkout({ item: "LostAckItem", quantity: 1, amount: 100 }, { commandId });
        return { success: true };
      } catch (err) {
        return { success: false, error: err };
      }
    }

    case "PROCESSING_ZERO_SIMULATION": {
      if (invariantSuite) invariantSuite.recordProcessingZeroBoundaryCheck();
      const { commandId } = op.params;
      const rawPayload = { item: "Proc0Item", quantity: 1, amount: 100 };
      adapters.commandStore.reserve({
        commandId,
        commandType: "CHECKOUT",
        payload: { ...rawPayload, simulateFailureAt: null },
      });
      try {
        engine.checkout(rawPayload, { commandId });
        return { success: true };
      } catch (err) {
        return { success: false, error: err };
      }
    }

    case "SCHEMA_UPCAST_TEST": {
      if (invariantSuite) invariantSuite.recordSchemaUpcastingCheck();
      const { EventUpcasterRegistry } = require("../../domain/eventUpcaster");
      const registry = new EventUpcasterRegistry();
      registry.register({
        eventType: "ORDER_CREATED",
        fromVersion: 1,
        toVersion: 2,
        upcast: (evt) => ({ ...evt, payload: { ...evt.payload, schemaUpcasted: true } }),
      });
      const v1Event = {
        eventId: "evt-upcast-test",
        eventType: "ORDER_CREATED",
        payload: { item: op.params.item, quantity: 1 },
        metadata: { schemaVersion: 1 },
      };
      const v2Event = registry.upcast(v1Event, 2);
      const isUpcast = v2Event.metadata.schemaVersion === 2 && v2Event.payload.schemaUpcasted === true;
      return { success: isUpcast, result: { upcast: isUpcast } };
    }

    default:
      return { success: false, error: new Error(`Unknown operation ${op.type}`) };
  }
}

module.exports = {
  MemoryProfileRunner,
};
