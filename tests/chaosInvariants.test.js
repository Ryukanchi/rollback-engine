const test = require("node:test");
const assert = require("node:assert/strict");
const { ChaosRunner } = require("../src/chaos/chaosRunner");

test("High-volume in-memory chaos campaign verifies all invariants without violations", () => {
  const seed = 123456;
  const result = ChaosRunner.runCampaign({
    seed,
    iterations: 200,
    profile: "memory",
  });

  assert.equal(result.violations, 0);
  assert.ok(result.operationsExecuted > 500);
  assert.ok(result.invariantCoverage.ReplayAuthority > 0);
  assert.ok(result.invariantCoverage.SnapshotEquivalence > 0);
  assert.ok(result.invariantCoverage.EventSequenceContiguous > 0);
  assert.ok(result.invariantCoverage.EventIdUniqueness > 0);
  assert.ok(result.invariantCoverage.TimestampMonotonicity > 0);
  assert.ok(result.invariantCoverage.ProjectionDeterminism > 0);
  assert.ok(result.invariantCoverage.MaterializedViewConsistency > 0);
  assert.ok(result.invariantCoverage.CompensationOrdering > 0);
  assert.ok(result.invariantCoverage.NoImpossibleFinalState > 0);
  assert.ok(result.invariantCoverage.CommandEventRangeConsistency > 0);
  assert.ok(result.invariantCoverage.DefensiveCopies > 0);
});
