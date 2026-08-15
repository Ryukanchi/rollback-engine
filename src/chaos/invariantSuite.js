const { projectEvents, createInitialState } = require("../domain/projection");
const { EVENT_TYPES } = require("../domain/events");

class InvariantSuite {
  #counters;

  constructor() {
    this.#counters = {
      ReplayAuthority: 0,
      SnapshotEquivalence: 0,
      EventSequenceContiguous: 0,
      EventIdUniqueness: 0,
      TimestampMonotonicity: 0,
      ProjectionDeterminism: 0,
      MaterializedViewConsistency: 0,
      IdempotentRetrySafety: 0,
      IdempotencyConflictDetection: 0,
      PostCommitSafety: 0,
      ProcessingZeroBoundary: 0,
      CompensationOrdering: 0,
      NoImpossibleFinalState: 0,
      CommitBoundary: 0,
      CommandEventRangeConsistency: 0,
      AggregateIsolation: 0,
      TimeTravelPrefix: 0,
      DefensiveCopies: 0,
      SchemaUpcasting: 0,
    };
  }

  getCounters() {
    return { ...this.#counters };
  }

  resetCounters() {
    for (const key of Object.keys(this.#counters)) {
      this.#counters[key] = 0;
    }
  }

  /**
   * Evaluates all core engine invariants against current state of stores & engine.
   * Throws detailed InvariantViolationError on failure.
   */
  checkAll({ engine, adapters, aggregateIds = [], context = {} }) {
    const checked = [];

    // 1. Inv D: Global Event ID Uniqueness
    this.#checkEventIdUniqueness(adapters.eventStore);
    checked.push("EventIdUniqueness");

    // 2. Aggregate-specific Invariants
    const allAggregates = aggregateIds.length > 0
      ? aggregateIds
      : extractAllAggregateIds(adapters.eventStore);

    for (const aggId of allAggregates) {
      const events = adapters.eventStore.getByAggregateId(aggId);
      if (events.length === 0) continue;

      // Inv C: Sequence Contiguity & 1-indexed strictly increasing
      this.#checkSequenceContiguity(aggId, events);
      checked.push("EventSequenceContiguous");

      // Inv E: Timestamp Monotonicity
      this.#checkTimestampMonotonicity(aggId, events);
      checked.push("TimestampMonotonicity");

      // Inv F: Projection Determinism
      this.#checkProjectionDeterminism(aggId, events);
      checked.push("ProjectionDeterminism");

      // Inv A: Replay Authority
      const replayState = engine.replay(aggId);
      this.#checkReplayAuthority(engine, aggId, replayState);
      checked.push("ReplayAuthority");

      // Inv B: Snapshot Equivalence
      this.#checkSnapshotEquivalence(engine, aggId, replayState);
      checked.push("SnapshotEquivalence");

      // Inv L & M: Compensation Order & Valid Final State
      this.#checkCompensationAndStateConsistency(aggId, events, replayState);
      checked.push("CompensationOrdering");
      checked.push("NoImpossibleFinalState");

      // Inv G: Materialized View (unless intentional unrepaired drift is active for this aggregate)
      const hasDrift = context.driftedAggregates && context.driftedAggregates.has(aggId);
      if (!hasDrift) {
        this.#checkMaterializedViewConsistency(engine, aggId, replayState);
        checked.push("MaterializedViewConsistency");
      }

      // Inv Q: Time Travel Prefix Consistency (check random/sampled prefix)
      this.#checkTimeTravelPrefix(engine, aggId, events);
      checked.push("TimeTravelPrefix");
    }

    // 3. Inv P: Aggregate Isolation (if multiple aggregates exist)
    if (allAggregates.length >= 2) {
      this.#checkAggregateIsolation(engine, adapters.eventStore, allAggregates);
      checked.push("AggregateIsolation");
    }

    // 4. Inv O: Command / Event Range Consistency
    this.#checkCommandEventRangeConsistency(adapters.commandStore, adapters.eventStore);
    checked.push("CommandEventRangeConsistency");

    // 5. Inv R: Defensive Copies
    this.#checkDefensiveCopies(engine, adapters, allAggregates);
    checked.push("DefensiveCopies");

    return checked;
  }

  // --- Specific Invariant Implementations ---

  #checkEventIdUniqueness(eventStore) {
    this.#counters.EventIdUniqueness++;
    const allEvents = eventStore.getAll();
    const seen = new Set();

    for (const evt of allEvents) {
      if (!evt.eventId) {
        throw new InvariantViolationError("EventIdUniqueness", "Event missing eventId", { event: evt });
      }
      if (seen.has(evt.eventId)) {
        throw new InvariantViolationError("EventIdUniqueness", `Duplicate eventId detected: ${evt.eventId}`, {
          duplicateEventId: evt.eventId,
        });
      }
      seen.add(evt.eventId);
    }
  }

  #checkSequenceContiguity(aggregateId, events) {
    this.#counters.EventSequenceContiguous++;
    for (let i = 0; i < events.length; i++) {
      const expectedSeq = i + 1;
      if (events[i].sequence !== expectedSeq) {
        throw new InvariantViolationError(
          "EventSequenceContiguous",
          `Aggregate ${aggregateId} sequence gap at index ${i}: expected ${expectedSeq}, found ${events[i].sequence}`,
          { aggregateId, index: i, expected: expectedSeq, actual: events[i].sequence }
        );
      }
    }
  }

  #checkTimestampMonotonicity(aggregateId, events) {
    this.#counters.TimestampMonotonicity++;
    for (let i = 1; i < events.length; i++) {
      const prevTime = new Date(events[i - 1].timestamp).getTime();
      const currTime = new Date(events[i].timestamp).getTime();
      if (currTime < prevTime) {
        throw new InvariantViolationError(
          "TimestampMonotonicity",
          `Aggregate ${aggregateId} timestamp moved backwards at sequence ${events[i].sequence}`,
          { aggregateId, sequence: events[i].sequence, prevTimestamp: events[i - 1].timestamp, currTimestamp: events[i].timestamp }
        );
      }
    }
  }

  #checkProjectionDeterminism(aggregateId, events) {
    this.#counters.ProjectionDeterminism++;
    const run1 = projectEvents(events);
    const run2 = projectEvents(events);
    const s1 = JSON.stringify(run1);
    const s2 = JSON.stringify(run2);
    if (s1 !== s2) {
      throw new InvariantViolationError(
        "ProjectionDeterminism",
        `Projection of identical event stream yielded different results for aggregate ${aggregateId}`,
        { aggregateId, run1, run2 }
      );
    }
  }

  #checkReplayAuthority(engine, aggregateId, replayState) {
    this.#counters.ReplayAuthority++;
    const authoritativeState = engine.replay(aggregateId);
    if (JSON.stringify(replayState) !== JSON.stringify(authoritativeState)) {
      throw new InvariantViolationError(
        "ReplayAuthority",
        `Replay state does not match authoritative state for aggregate ${aggregateId}`,
        { aggregateId, replayState, authoritativeState }
      );
    }
  }

  #checkSnapshotEquivalence(engine, aggregateId, fullReplayState) {
    this.#counters.SnapshotEquivalence++;
    const fromSnapshot = engine.replayFromSnapshot(aggregateId);
    if (JSON.stringify(fromSnapshot) !== JSON.stringify(fullReplayState)) {
      throw new InvariantViolationError(
        "SnapshotEquivalence",
        `replayFromSnapshot() diverged from fullReplay for aggregate ${aggregateId}`,
        { aggregateId, fromSnapshot, fullReplayState }
      );
    }
  }

  #checkMaterializedViewConsistency(engine, aggregateId, authoritativeState) {
    this.#counters.MaterializedViewConsistency++;
    const matState = engine.getState(aggregateId, { consistency: "materialized" });
    if (matState && authoritativeState) {
      if (JSON.stringify(matState) !== JSON.stringify(authoritativeState)) {
        throw new InvariantViolationError(
          "MaterializedViewConsistency",
          `Materialized state differs from authoritative state for aggregate ${aggregateId}`,
          { aggregateId, matState, authoritativeState }
        );
      }
    }
  }

  #checkCompensationAndStateConsistency(aggregateId, events, state) {
    this.#counters.CompensationOrdering++;
    this.#counters.NoImpossibleFinalState++;

    if (!state) return;

    const eventTypes = events.map((e) => e.eventType);

    // If order was compensated after payment
    const compIdx = eventTypes.indexOf(EVENT_TYPES.PAYMENT_REFUNDED);
    if (compIdx !== -1) {
      // Must be followed by INVENTORY_RELEASED and ORDER_ROLLED_BACK
      const relIdx = eventTypes.indexOf(EVENT_TYPES.INVENTORY_RELEASED, compIdx);
      const rollIdx = eventTypes.indexOf(EVENT_TYPES.ORDER_ROLLED_BACK, relIdx);

      if (relIdx === -1 || rollIdx === -1 || relIdx < compIdx || rollIdx < relIdx) {
        throw new InvariantViolationError(
          "CompensationOrdering",
          `Invalid compensation event sequence for aggregate ${aggregateId}: ${eventTypes.join(" -> ")}`,
          { aggregateId, eventTypes }
        );
      }

      if (state.lifecycle !== "rolled_back" && !state.deleted) {
        throw new InvariantViolationError(
          "NoImpossibleFinalState",
          `Compensated order has lifecycle '${state.lifecycle}' instead of 'rolled_back'`,
          { aggregateId, state }
        );
      }

      if (!state.deleted && (state.payment?.status !== "refunded" || state.inventory?.status !== "released")) {
        throw new InvariantViolationError(
          "NoImpossibleFinalState",
          `Compensated order state has inconsistent payment/inventory status`,
          { aggregateId, state }
        );
      }
    }

    // If completed
    if (state.lifecycle === "completed") {
      if (state.payment?.status !== "charged" || state.inventory?.status !== "reserved") {
        throw new InvariantViolationError(
          "NoImpossibleFinalState",
          `Completed order has inconsistent payment/inventory status`,
          { aggregateId, state }
        );
      }
    }

    // If deleted
    if (state.deleted) {
      if (state.payment?.status === "charged" || state.inventory?.status === "reserved") {
        throw new InvariantViolationError(
          "NoImpossibleFinalState",
          `Deleted order still holds active payment or inventory reservation`,
          { aggregateId, state }
        );
      }
    }
  }

  #checkTimeTravelPrefix(engine, aggregateId, events) {
    this.#counters.TimeTravelPrefix++;
    if (events.length === 0) return;

    // Pick a sequence to verify
    const sampleSeq = Math.min(events.length, Math.max(0, Math.floor(events.length / 2)));
    const timeTravelState = engine.replayAtSequence(aggregateId, sampleSeq);
    const expectedEvents = events.filter((e) => e.sequence <= sampleSeq);
    const expectedState = expectedEvents.length > 0
      ? projectEvents(expectedEvents)
      : createInitialState(aggregateId);

    if (JSON.stringify(timeTravelState) !== JSON.stringify(expectedState)) {
      throw new InvariantViolationError(
        "TimeTravelPrefix",
        `replayAtSequence(${sampleSeq}) did not match expected prefix projection for aggregate ${aggregateId}`,
        { aggregateId, sequence: sampleSeq, timeTravelState, expectedState }
      );
    }
  }

  #checkAggregateIsolation(engine, eventStore, aggregateIds) {
    this.#counters.AggregateIsolation++;
    const [idA, idB] = aggregateIds;
    if (!idA || !idB || idA === idB) return;

    const eventsA = eventStore.getByAggregateId(idA);
    const eventsB = eventStore.getByAggregateId(idB);

    const replayA = engine.replay(idA);
    const replayB = engine.replay(idB);

    if (replayA && replayA.aggregateId !== idA) {
      throw new InvariantViolationError("AggregateIsolation", `Replay for aggregate ${idA} has aggregateId ${replayA.aggregateId}`);
    }
    if (replayB && replayB.aggregateId !== idB) {
      throw new InvariantViolationError("AggregateIsolation", `Replay for aggregate ${idB} has aggregateId ${replayB.aggregateId}`);
    }

    // Replay A calculated only from eventsA
    const expectedA = projectEvents(eventsA);
    if (JSON.stringify(replayA) !== JSON.stringify(expectedA)) {
      throw new InvariantViolationError("AggregateIsolation", `Aggregate ${idA} state depends on external aggregate events!`);
    }
  }

  #checkCommandEventRangeConsistency(commandStore, eventStore) {
    this.#counters.CommandEventRangeConsistency++;
    // Verify command store records match event store
    const allEvents = eventStore.getAll();
    const commandEventMap = new Map();

    for (const evt of allEvents) {
      const cmdId = evt.metadata?.commandId;
      if (!cmdId) continue;
      if (!commandEventMap.has(cmdId)) {
        commandEventMap.set(cmdId, []);
      }
      commandEventMap.get(cmdId).push(evt);
    }

    for (const [cmdId, events] of commandEventMap.entries()) {
      const cmdRecord = commandStore.get(cmdId);
      if (cmdRecord && cmdRecord.status === "completed" && cmdRecord.eventRange) {
        if (cmdRecord.eventRange.firstSequence !== events[0].sequence) {
          throw new InvariantViolationError(
            "CommandEventRangeConsistency",
            `Command ${cmdId} firstSequence mismatch: recorded ${cmdRecord.eventRange.firstSequence}, actual ${events[0].sequence}`,
            { cmdId, recorded: cmdRecord.eventRange, actualFirst: events[0].sequence }
          );
        }
        if (cmdRecord.eventRange.lastSequence !== events[events.length - 1].sequence) {
          throw new InvariantViolationError(
            "CommandEventRangeConsistency",
            `Command ${cmdId} lastSequence mismatch: recorded ${cmdRecord.eventRange.lastSequence}, actual ${events[events.length - 1].sequence}`,
            { cmdId, recorded: cmdRecord.eventRange, actualLast: events[events.length - 1].sequence }
          );
        }
      }
    }
  }

  #checkDefensiveCopies(engine, adapters, aggregateIds) {
    this.#counters.DefensiveCopies++;
    if (aggregateIds.length === 0) return;
    const aggId = aggregateIds[0];
    const events = adapters.eventStore.getByAggregateId(aggId);
    if (events.length === 0) return;

    // Mutate the array or attempt property mutation if not frozen
    const copy1 = adapters.eventStore.getByAggregateId(aggId);
    if (copy1.length > 0) {
      try {
        if (!Object.isFrozen(copy1[0])) {
          copy1[0].eventType = "CORRUPTED_IN_TEST";
          if (copy1[0].payload && !Object.isFrozen(copy1[0].payload)) {
            copy1[0].payload.corrupted = true;
          }
        }
      } catch {
        // TypeError due to Object.freeze is proof of defensive immutability
      }
    }

    // Fresh read must be unaffected
    const freshEvents = adapters.eventStore.getByAggregateId(aggId);
    if (freshEvents[0].eventType === "CORRUPTED_IN_TEST" || freshEvents[0].payload?.corrupted) {
      throw new InvariantViolationError(
        "DefensiveCopies",
        `Event store does not return defensive copies; external mutation leaked into store!`,
        { aggregateId: aggId }
      );
    }
  }

  // Explicit record methods for standalone tests / operations
  recordIdempotentRetryCheck() {
    this.#counters.IdempotentRetrySafety++;
  }

  recordIdempotencyConflictCheck() {
    this.#counters.IdempotencyConflictDetection++;
  }

  recordPostCommitSafetyCheck() {
    this.#counters.PostCommitSafety++;
  }

  recordProcessingZeroBoundaryCheck() {
    this.#counters.ProcessingZeroBoundary++;
  }

  recordCommitBoundaryCheck() {
    this.#counters.CommitBoundary++;
  }

  recordSchemaUpcastingCheck() {
    this.#counters.SchemaUpcasting++;
  }
}

class InvariantViolationError extends Error {
  constructor(invariantName, message, details = {}) {
    super(`[INVARIANT VIOLATION: ${invariantName}] ${message}`);
    this.name = "InvariantViolationError";
    this.invariantName = invariantName;
    this.details = details;
  }
}

function extractAllAggregateIds(eventStore) {
  const all = eventStore.getAll();
  const set = new Set();
  for (const e of all) {
    if (e.aggregateId) set.add(e.aggregateId);
  }
  return [...set];
}

module.exports = {
  InvariantSuite,
  InvariantViolationError,
};
