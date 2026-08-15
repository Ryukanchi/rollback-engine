const { scenarioStore } = require("./scenarioStore");
const { runSuccessfulCheckoutScenario } = require("./scenarios/successScenario");
const { runCompensationScenario } = require("./scenarios/compensationScenario");
const { runReadModelDriftScenario, repairReadModelDrift } = require("./scenarios/readModelDriftScenario");
const { runReconciliationScenario } = require("./scenarios/reconciliationScenario");
const { runBoundaryScenario } = require("./scenarios/boundaryScenario");
const { runRestartScenario } = require("./scenarios/restartScenario");

const SCENARIO_RUNNERS = {
  successful_checkout: runSuccessfulCheckoutScenario,
  compensation_after_payment: runCompensationScenario,
  read_model_drift: runReadModelDriftScenario,
  post_commit_reconciliation: runReconciliationScenario,
  processing_zero_events_boundary: runBoundaryScenario,
  process_restart_durability: runRestartScenario,
};

function runScenario({ scenarioType = "successful_checkout", storageType = "sqlite", options = {} } = {}) {
  const runner = SCENARIO_RUNNERS[scenarioType];
  if (!runner) {
    throw new TypeError(`Unknown scenario type: ${scenarioType}. Available: ${Object.keys(SCENARIO_RUNNERS).join(", ")}`);
  }

  const session = scenarioStore.createSession({ scenarioType, storageType });
  const result = runner(session, options);
  session.lastResult = result;

  return result;
}

function repairScenario(scenarioId) {
  const session = scenarioStore.getSession(scenarioId);
  if (!session) {
    throw new Error(`Scenario session not found or expired: ${scenarioId}`);
  }

  if (session.scenarioType !== "read_model_drift") {
    throw new Error(`Scenario ${scenarioId} is not a read_model_drift scenario`);
  }

  const result = repairReadModelDrift(session);
  session.lastResult = result;
  return result;
}

function replayScenarioSequence(scenarioId, sequenceNumber) {
  const session = scenarioStore.getSession(scenarioId);
  if (!session) {
    throw new Error(`Scenario session not found or expired: ${scenarioId}`);
  }

  const lastResult = session.lastResult;
  if (!lastResult || !lastResult.aggregateId) {
    throw new Error(`No active aggregate stream for scenario: ${scenarioId}`);
  }

  const seq = Number(sequenceNumber);
  if (!Number.isSafeInteger(seq) || seq < 0) {
    throw new TypeError("Sequence must be a non-negative integer");
  }

  const maxSeq = lastResult.events?.length || 0;
  if (seq > maxSeq) {
    throw new RangeError(`Requested sequence ${seq} exceeds maximum committed sequence ${maxSeq}`);
  }

  let state;
  let previousState;

  if (seq === 0) {
    state = {
      aggregateId: lastResult.aggregateId,
      version: 0,
      lifecycle: "initial",
      order: null,
      inventory: null,
      payment: null,
    };
  } else {
    state = session.engine.replayAtSequence(lastResult.aggregateId, seq);
  }

  if (seq <= 1) {
    previousState = {
      aggregateId: lastResult.aggregateId,
      version: 0,
      lifecycle: "initial",
      order: null,
      inventory: null,
      payment: null,
    };
  } else {
    previousState = session.engine.replayAtSequence(lastResult.aggregateId, seq - 1);
  }

  return {
    scenarioId,
    aggregateId: lastResult.aggregateId,
    sequence: seq,
    maxSequence: maxSeq,
    state,
    previousState,
  };
}

function getScenarioResult(scenarioId) {
  const session = scenarioStore.getSession(scenarioId);
  if (!session || !session.lastResult) {
    return null;
  }
  return session.lastResult;
}

module.exports = {
  runScenario,
  repairScenario,
  replayScenarioSequence,
  getScenarioResult,
};
