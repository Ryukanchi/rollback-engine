const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { createStorageAdapters } = require("../../infrastructure/storageFactory");
const { RollbackEngine } = require("../../application/rollbackEngine");
const { ExecutionTrace } = require("../executionTrace");
const { classifyOutcome } = require("../failureClassifier");

class SqliteProfileRunner {
  static runIteration({ seed, iteration, prng, generator, invariantSuite }) {
    const trace = new ExecutionTrace({ seed, iteration, profile: "sqlite" });
    const dbFileName = `rollback-chaos-${seed}-${iteration}-${prng.nextString(6)}.db`;
    const dbPath = path.join(os.tmpdir(), dbFileName);

    let adapters = createStorageAdapters({ type: "sqlite", dbPath });
    let engine = new RollbackEngine({
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
      supportsSnapshotDelete: true,
      dbPath,
    };

    const stats = {
      operations: 0,
      domainRejections: 0,
      failuresInjected: 0,
      retries: 0,
      recoveries: 0,
      reopens: 0,
    };

    const numOps = prng.nextInt(4, 8);

    try {
      for (let step = 0; step < numOps; step++) {
        // Occasionally inject a database close & reopen across connection boundary
        if (step > 0 && prng.nextBoolean(0.2)) {
          adapters.close();
          adapters = createStorageAdapters({ type: "sqlite", dbPath });
          engine = new RollbackEngine({
            eventStore: adapters.eventStore,
            commandStore: adapters.commandStore,
            snapshotStore: adapters.snapshotStore,
            stateRepository: adapters.stateRepository,
          });
          stats.reopens++;
          trace.recordStep({
            opName: "REOPEN_DATABASE",
            args: { dbPath },
            outcome: { success: true },
            classification: "SUCCESS",
            invariantsChecked: [],
          });
        }

        const op = generator.generateOperation(context);
        stats.operations++;

        let outcome = { success: false, result: null, error: null };
        let invariantsChecked = [];

        try {
          outcome = executeSqliteOperation({ op, engine, adapters, context, stats, invariantSuite });
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

        // Check invariants
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
    } finally {
      // Safe cleanup of SQLite temporary files
      try {
        adapters.close();
      } catch {}
      cleanupSqliteFiles(dbPath);
    }
  }
}

function executeSqliteOperation({ op, engine, adapters, context, stats, invariantSuite }) {
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
      context.driftedAggregates.delete(op.params.aggregateId); // Self-heals!
      return { success: true, result: res };
    }

    case "MATERIALIZED_READ": {
      const res = engine.getState(op.params.aggregateId, { consistency: "materialized" });
      return { success: true, result: res };
    }

    case "RECOVER": {
      stats.recoveries++;
      const res = engine.recover(op.params.aggregateId);
      context.driftedAggregates.delete(op.params.aggregateId); // Rebuilds view!
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
      if (current && adapters.db) {
        const corrupted = {
          ...current,
          order: { ...current.order, item: "CORRUPTED_SQLITE_CACHE_ROW", quantity: 9999 },
        };
        adapters.db
          .prepare("UPDATE materialized_states SET state = ? WHERE aggregate_id = ?")
          .run(JSON.stringify(corrupted), aggregateId);
        context.driftedAggregates.add(aggregateId);
      }
      return { success: true, result: { corrupted: true } };
    }

    case "VIEW_DELETE": {
      const { aggregateId } = op.params;
      if (adapters.db) {
        adapters.db.prepare("DELETE FROM materialized_states WHERE aggregate_id = ?").run(aggregateId);
        context.driftedAggregates.add(aggregateId);
      }
      return { success: true, result: { viewDeleted: true } };
    }

    case "SNAPSHOT_CORRUPT": {
      const { aggregateId } = op.params;
      if (adapters.db) {
        adapters.db
          .prepare("UPDATE snapshots SET version = version + 999 WHERE aggregate_id = ?")
          .run(aggregateId);
      }
      return { success: true, result: { snapshotCorrupted: true } };
    }

    case "SNAPSHOT_DELETE": {
      const { aggregateId } = op.params;
      if (adapters.db) {
        adapters.db.prepare("DELETE FROM snapshots WHERE aggregate_id = ?").run(aggregateId);
      }
      return { success: true, result: { snapshotDeleted: true } };
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
      const payload = { item: "LostAckItem", quantity: 1, amount: 100, simulateFailureAt: null };

      adapters.commandStore.reserve({ commandId, commandType: "CHECKOUT", payload });
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
        eventId: "evt-upcast-sqlite-test",
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

function cleanupSqliteFiles(dbPath) {
  const files = [dbPath, `${dbPath}-wal`, `${dbPath}-shm`];
  for (const f of files) {
    try {
      if (fs.existsSync(f)) {
        fs.unlinkSync(f);
      }
    } catch {}
  }
}

module.exports = {
  SqliteProfileRunner,
};
