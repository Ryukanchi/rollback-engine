const test = require("node:test");
const assert = require("node:assert/strict");
const { ReproWriter } = require("../src/chaos/reproWriter");
const { ExecutionTrace } = require("../src/chaos/executionTrace");
const { InvariantViolationError } = require("../src/chaos/invariantSuite");

test("ReproWriter formats informative failure reports with seed, trace, and reproduction command", () => {
  const seed = 998877;
  const iteration = 42;
  const profile = "memory";

  const trace = new ExecutionTrace({ seed, iteration, profile });
  trace.recordStep({
    opName: "CHECKOUT",
    args: { payload: { item: "LaserEmitter", quantity: 2 } },
    outcome: { success: true },
    classification: "SUCCESS",
  });
  trace.recordStep({
    opName: "VIEW_CORRUPT",
    args: { aggregateId: 1 },
    outcome: { success: true },
    classification: "SUCCESS",
  });

  const violation = new InvariantViolationError(
    "MaterializedViewConsistency",
    "Materialized state differs from authoritative state",
    { aggregateId: 1, expected: "active", actual: "corrupted" }
  );

  const report = ReproWriter.formatFailureReport({
    seed,
    iteration,
    profile,
    violatedInvariant: "MaterializedViewConsistency",
    error: violation,
    trace,
    affectedAggregateId: 1,
  });

  assert.ok(report.includes("CHAOS INVARIANT VIOLATION DETECTED"));
  assert.ok(report.includes(`Seed:        ${seed}`));
  assert.ok(report.includes(`Iteration:   ${iteration}`));
  assert.ok(report.includes("Invariant:   MaterializedViewConsistency"));
  assert.ok(report.includes(`npm run chaos -- --seed=${seed} --iteration=${iteration} --profile=${profile}`));
  assert.ok(report.includes("01 CHECKOUT"));
  assert.ok(report.includes("02 VIEW_CORRUPT"));
});
