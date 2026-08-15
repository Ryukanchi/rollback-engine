const test = require("node:test");
const assert = require("node:assert/strict");
const { ChaosRunner } = require("../src/chaos/chaosRunner");
const { MemoryProfileRunner } = require("../src/chaos/profiles/memoryProfile");
const { SeededRandom } = require("../src/chaos/seededRandom");
const { OperationGenerator } = require("../src/chaos/operationGenerator");
const { InvariantSuite } = require("../src/chaos/invariantSuite");

test("Direct single-iteration reproduction mode executes exact identical steps", () => {
  const seed = 778899;
  const targetIteration = 25;

  // Run single iteration mode
  const singleRun = ChaosRunner.runCampaign({
    seed,
    iteration: targetIteration,
    profile: "memory",
  });

  // Run manually derived iteration
  const iterPrng = new SeededRandom(seed ^ (targetIteration * 0x9e3779b9));
  const iterGen = new OperationGenerator(iterPrng);
  const suite = new InvariantSuite();

  const manualRun = MemoryProfileRunner.runIteration({
    seed,
    iteration: targetIteration,
    prng: iterPrng,
    generator: iterGen,
    invariantSuite: suite,
  });

  assert.equal(singleRun.violations, 0);
  assert.equal(manualRun.success, true);
  assert.equal(singleRun.operationsExecuted, manualRun.stats.operations);
});
