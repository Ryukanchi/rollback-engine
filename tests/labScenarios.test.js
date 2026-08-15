const test = require("node:test");
const assert = require("node:assert/strict");
const { existsSync } = require("node:fs");

const {
  runScenario,
  repairScenario,
  replayScenarioSequence,
  getScenarioResult,
} = require("../src/lab/scenarioRunner");
const { scenarioStore } = require("../src/lab/scenarioStore");
const { EVENT_TYPES } = require("../src/domain/events");

test("lab scenario: successful checkout produces 3 events and completed lifecycle", () => {
  const result = runScenario({ scenarioType: "successful_checkout" });

  assert.equal(result.status, "completed");
  assert.equal(result.events.length, 3);
  assert.deepEqual(
    result.events.map((e) => e.eventType),
    [
      EVENT_TYPES.ORDER_CREATED,
      EVENT_TYPES.INVENTORY_RESERVED,
      EVENT_TYPES.PAYMENT_CHARGED,
    ]
  );
  assert.equal(result.finalState.lifecycle, "completed");
  assert.equal(result.finalState.order.item, "ServerRack");

  // Invariants verification
  assert.equal(result.invariants.sequenceContiguous.passed, true);
  assert.equal(result.invariants.eventIdsUnique.passed, true);
  assert.equal(result.invariants.replayAuthoritativeMatch.passed, true);
  assert.equal(result.invariants.viewSynchronized.passed, true);
});

test("lab scenario: compensation produces 6 events and rolled_back lifecycle", () => {
  const result = runScenario({ scenarioType: "compensation_after_payment" });

  assert.equal(result.status, "rolled_back");
  assert.equal(result.events.length, 6);
  assert.deepEqual(
    result.events.map((e) => e.eventType),
    [
      EVENT_TYPES.ORDER_CREATED,
      EVENT_TYPES.INVENTORY_RESERVED,
      EVENT_TYPES.PAYMENT_CHARGED,
      EVENT_TYPES.PAYMENT_REFUNDED,
      EVENT_TYPES.INVENTORY_RELEASED,
      EVENT_TYPES.ORDER_ROLLED_BACK,
    ]
  );
  assert.equal(result.finalState.lifecycle, "rolled_back");
  assert.equal(result.finalState.payment.status, "refunded");
  assert.equal(result.finalState.inventory.status, "released");

  // Invariants
  assert.equal(result.invariants.sequenceContiguous.passed, true);
  assert.equal(result.invariants.eventIdsUnique.passed, true);
  assert.equal(result.invariants.replayAuthoritativeMatch.passed, true);
});

test("lab scenario: read model drift is detected and repaired via authoritative replay", () => {
  // Step 1: Run drift scenario
  const driftResult = runScenario({ scenarioType: "read_model_drift" });

  assert.equal(driftResult.status, "drift_detected");
  assert.equal(driftResult.drift.detected, true);
  assert.equal(driftResult.materializedState.order.item, "MUTATED_STALE_CACHE");
  assert.equal(driftResult.authoritativeState.order.item, "PrecisionCalibrator");
  assert.equal(driftResult.invariants.viewSynchronized.passed, false);

  // Step 2: Repair scenario
  const repairResult = repairScenario(driftResult.scenarioId);

  assert.equal(repairResult.status, "repaired");
  assert.equal(repairResult.drift.detected, false);
  assert.equal(repairResult.materializedState.order.item, "PrecisionCalibrator");
  assert.equal(repairResult.invariants.viewSynchronized.passed, true);
});

test("lab scenario: post-commit reconciliation prevents duplicate events on retry", () => {
  const result = runScenario({ scenarioType: "post_commit_reconciliation" });

  assert.equal(result.status, "reconciliation_prevented_duplicate");
  assert.equal(result.reconciliation.interruptedCommitDetected, true);
  assert.equal(result.reconciliation.errorCode, "COMMAND_EXECUTION_INTERRUPTED_AFTER_COMMIT");
  assert.equal(result.reconciliation.eventsBeforeRetry, 1);
  assert.equal(result.reconciliation.eventsAfterRetry, 1);
  assert.equal(result.reconciliation.duplicateEventsPrevented, true);
  assert.equal(result.invariants.duplicateRetryEvents.passed, true);
});

test("lab scenario: processing + 0 events boundary safely refuses automatic takeover", () => {
  const result = runScenario({ scenarioType: "processing_zero_events_boundary" });

  assert.equal(result.status, "boundary_enforced");
  assert.equal(result.boundary.enforced, true);
  assert.equal(result.boundary.errorCode, "COMMAND_IN_PROGRESS");
  assert.equal(result.boundary.retryAction, "WAIT_AND_RETRY_SAME_KEY");
  assert.equal(result.boundary.eventsCreated, 0);
});

test("lab scenario: process restart durability survives across separate OS child processes", () => {
  const result = runScenario({
    scenarioType: "process_restart_durability",
    options: { mode: "standard" },
  });

  assert.equal(result.status, "completed");
  assert.equal(result.restart.processA.exitCode, 0);
  assert.equal(result.restart.processB.exitCode, 0);
  assert.equal(result.restart.stateMatch, true);
  assert.equal(result.restart.processBoundaryVerified, true);
  assert.equal(result.events.length, 3);
});

test("lab scenario: sequence time travel accurately reconstructs state at each sequence", () => {
  const result = runScenario({ scenarioType: "compensation_after_payment" });
  const scenarioId = result.scenarioId;

  // Seq 0: Initial
  const seq0 = replayScenarioSequence(scenarioId, 0);
  assert.equal(seq0.sequence, 0);
  assert.equal(seq0.state.lifecycle, "initial");

  // Seq 1: ORDER_CREATED
  const seq1 = replayScenarioSequence(scenarioId, 1);
  assert.equal(seq1.sequence, 1);
  assert.equal(seq1.state.lifecycle, "active");
  assert.equal(seq1.state.order.item, "HighEndGPU");
  assert.equal(seq1.state.inventory, null);

  // Seq 2: INVENTORY_RESERVED
  const seq2 = replayScenarioSequence(scenarioId, 2);
  assert.equal(seq2.sequence, 2);
  assert.equal(seq2.state.inventory.status, "reserved");
  assert.equal(seq2.state.payment, null);

  // Seq 3: PAYMENT_CHARGED
  const seq3 = replayScenarioSequence(scenarioId, 3);
  assert.equal(seq3.sequence, 3);
  assert.equal(seq3.state.payment.status, "charged");

  // Seq 4: PAYMENT_REFUNDED
  const seq4 = replayScenarioSequence(scenarioId, 4);
  assert.equal(seq4.sequence, 4);
  assert.equal(seq4.state.payment.status, "refunded");

  // Seq 5: INVENTORY_RELEASED
  const seq5 = replayScenarioSequence(scenarioId, 5);
  assert.equal(seq5.sequence, 5);
  assert.equal(seq5.state.inventory.status, "released");

  // Seq 6: ORDER_ROLLED_BACK
  const seq6 = replayScenarioSequence(scenarioId, 6);
  assert.equal(seq6.sequence, 6);
  assert.equal(seq6.state.lifecycle, "rolled_back");

  // Error handling
  assert.throws(() => replayScenarioSequence(scenarioId, -1), TypeError);
  assert.throws(() => replayScenarioSequence(scenarioId, 99), RangeError);
});

test("scenarioStore cleanly deletes temp SQLite database files on session close", () => {
  const session = scenarioStore.createSession({ scenarioType: "custom", storageType: "sqlite" });
  const dbPath = session.dbPath;

  // DB file exists
  assert.equal(existsSync(dbPath), true);

  // Close session
  const closed = scenarioStore.closeSession(session.scenarioId);
  assert.equal(closed, true);

  // DB file is removed
  assert.equal(existsSync(dbPath), false);
});
