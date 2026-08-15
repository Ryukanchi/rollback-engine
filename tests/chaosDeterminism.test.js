const test = require("node:test");
const assert = require("node:assert/strict");
const { ChaosRunner } = require("../src/chaos/chaosRunner");
const { SeededRandom } = require("../src/chaos/seededRandom");
const { OperationGenerator } = require("../src/chaos/operationGenerator");

test("SeededRandom generates 100% deterministic sequence for the same seed", () => {
  const seed = 987654;
  const rng1 = new SeededRandom(seed);
  const rng2 = new SeededRandom(seed);

  for (let i = 0; i < 100; i++) {
    assert.equal(rng1.next(), rng2.next());
    assert.equal(rng1.nextInt(1, 1000), rng2.nextInt(1, 1000));
    assert.equal(rng1.nextBoolean(), rng2.nextBoolean());
    assert.equal(rng1.nextString(12), rng2.nextString(12));
  }
});

test("SeededRandom generates different sequence for different seeds", () => {
  const rng1 = new SeededRandom(11111);
  const rng2 = new SeededRandom(99999);

  const seq1 = Array.from({ length: 10 }, () => rng1.next());
  const seq2 = Array.from({ length: 10 }, () => rng2.next());

  assert.notDeepEqual(seq1, seq2);
});

test("OperationGenerator produces identical operation sequence given the same seed", () => {
  const seed = 482971;
  const gen1 = new OperationGenerator(new SeededRandom(seed));
  const gen2 = new OperationGenerator(new SeededRandom(seed));

  const plan1 = gen1.generateScenarioPlan(10);
  const plan2 = gen2.generateScenarioPlan(10);

  assert.deepEqual(plan1, plan2);
});

test("Chaos campaign execution with same seed produces identical totals and checks", () => {
  const seed = 555444;
  const run1 = ChaosRunner.runCampaign({ seed, iterations: 50, profile: "memory" });
  const run2 = ChaosRunner.runCampaign({ seed, iterations: 50, profile: "memory" });

  assert.equal(run1.operationsExecuted, run2.operationsExecuted);
  assert.equal(run1.expectedDomainRejections, run2.expectedDomainRejections);
  assert.equal(run1.injectedFailures, run2.injectedFailures);
  assert.equal(run1.retries, run2.retries);
  assert.equal(run1.recoveries, run2.recoveries);
  assert.deepEqual(run1.invariantCoverage, run2.invariantCoverage);
});
