const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { ChaosRunner } = require("../src/chaos/chaosRunner");

test("Persistent SQLite chaos campaign survives reopens, drift, and cleans up database files", () => {
  const seed = 654321;
  const result = ChaosRunner.runCampaign({
    seed,
    iterations: 30,
    profile: "sqlite",
  });

  assert.equal(result.violations, 0);
  assert.ok(result.operationsExecuted > 50);
  assert.ok(result.reopens > 0, "Should have executed at least one DB close/reopen cycle");
  assert.ok(result.invariantCoverage.ReplayAuthority > 0);
  assert.ok(result.invariantCoverage.MaterializedViewConsistency > 0);

  // Verify no orphaned temporary chaos DB files in os.tmpdir()
  const tmpFiles = fs.readdirSync(os.tmpdir());
  const orphaned = tmpFiles.filter((f) => f.startsWith(`rollback-chaos-${seed}`));
  assert.equal(orphaned.length, 0, `Expected 0 orphaned SQLite temp files, found: ${orphaned.join(", ")}`);
});
