const { SeededRandom } = require("./seededRandom");
const { OperationGenerator } = require("./operationGenerator");
const { InvariantSuite } = require("./invariantSuite");
const { ReproWriter } = require("./reproWriter");
const { MemoryProfileRunner } = require("./profiles/memoryProfile");
const { SqliteProfileRunner } = require("./profiles/sqliteProfile");

const CAMPAIGNS = {
  smoke: { memoryIterations: 100, sqliteIterations: 10 },
  standard: { memoryIterations: 5000, sqliteIterations: 250 },
  sqlite: { memoryIterations: 0, sqliteIterations: 500 },
  stress: { memoryIterations: 15000, sqliteIterations: 1000 },
};

class ChaosRunner {
  static runCampaign(options = {}) {
    const seed = options.seed !== undefined ? options.seed : (Date.now() ^ (Math.random() * 0x100000000)) >>> 0;
    const campaignName = options.campaign || (options.iterations ? "custom" : "standard");
    const campaignConfig = CAMPAIGNS[campaignName] || CAMPAIGNS.standard;

    let memoryIterations = campaignConfig.memoryIterations;
    let sqliteIterations = campaignConfig.sqliteIterations;

    if (options.profile === "memory") {
      memoryIterations = options.iterations || memoryIterations || 5000;
      sqliteIterations = 0;
    } else if (options.profile === "sqlite") {
      sqliteIterations = options.iterations || sqliteIterations || 250;
      memoryIterations = 0;
    } else if (options.iterations) {
      if (memoryIterations > 0 && sqliteIterations > 0) {
        memoryIterations = options.iterations;
        sqliteIterations = Math.max(10, Math.floor(options.iterations * 0.05));
      } else if (sqliteIterations > 0) {
        sqliteIterations = options.iterations;
      } else {
        memoryIterations = options.iterations;
      }
    }

    const prng = new SeededRandom(seed);
    const invariantSuite = new InvariantSuite();
    const generator = new OperationGenerator(prng);

    const startTime = Date.now();
    const totals = {
      seed,
      campaign: campaignName,
      profile: options.profile || "all",
      memoryIterations,
      sqliteIterations,
      totalIterations: memoryIterations + sqliteIterations,
      operationsExecuted: 0,
      expectedDomainRejections: 0,
      injectedFailures: 0,
      retries: 0,
      recoveries: 0,
      reopens: 0,
      invariantChecksCount: 0,
      violations: 0,
      failureReport: null,
      runtimeMs: 0,
    };

    // Single iteration reproduction mode
    if (options.iteration !== undefined) {
      const targetIter = Number(options.iteration);
      const isSqlite = options.profile === "sqlite";
      const iterPrng = new SeededRandom(seed ^ (targetIter * 0x9e3779b9));
      const iterGenerator = new OperationGenerator(iterPrng);

      const runner = isSqlite ? SqliteProfileRunner : MemoryProfileRunner;
      const res = runner.runIteration({
        seed,
        iteration: targetIter,
        prng: iterPrng,
        generator: iterGenerator,
        invariantSuite,
      });

      totals.runtimeMs = Date.now() - startTime;
      totals.totalIterations = 1;
      accumulateStats(totals, res.stats);

      if (!res.success) {
        totals.violations = 1;
        totals.failureReport = ReproWriter.formatFailureReport({
          seed,
          iteration: targetIter,
          profile: isSqlite ? "sqlite" : "memory",
          violatedInvariant: res.violatedInvariant,
          error: res.error,
          trace: res.trace,
          engine: res.engine,
          adapters: res.adapters,
          affectedAggregateId: res.affectedAggregateId,
        });
        ReproWriter.writeFailureArtifact({
          seed,
          iteration: targetIter,
          profile: isSqlite ? "sqlite" : "memory",
          violatedInvariant: res.violatedInvariant,
          error: res.error,
          trace: res.trace,
        });
        return totals;
      }

      totals.invariantCoverage = invariantSuite.getCounters();
      return totals;
    }

    // 1. Run High-Volume In-Memory Profile
    for (let i = 1; i <= memoryIterations; i++) {
      // Deterministically derive child PRNG for this iteration so each iteration is individually reproducible
      const iterPrng = new SeededRandom(seed ^ (i * 0x9e3779b9));
      const iterGenerator = new OperationGenerator(iterPrng);

      const res = MemoryProfileRunner.runIteration({
        seed,
        iteration: i,
        prng: iterPrng,
        generator: iterGenerator,
        invariantSuite,
      });

      accumulateStats(totals, res.stats);

      if (!res.success) {
        totals.violations++;
        totals.runtimeMs = Date.now() - startTime;
        totals.failureReport = ReproWriter.formatFailureReport({
          seed,
          iteration: i,
          profile: "memory",
          violatedInvariant: res.violatedInvariant,
          error: res.error,
          trace: res.trace,
          engine: res.engine,
          adapters: res.adapters,
          affectedAggregateId: res.affectedAggregateId,
        });
        ReproWriter.writeFailureArtifact({
          seed,
          iteration: i,
          profile: "memory",
          violatedInvariant: res.violatedInvariant,
          error: res.error,
          trace: res.trace,
        });
        return totals;
      }
    }

    // 2. Run Persistent SQLite Profile
    for (let i = 1; i <= sqliteIterations; i++) {
      const iterIndex = memoryIterations + i;
      const iterPrng = new SeededRandom(seed ^ (iterIndex * 0x9e3779b9));
      const iterGenerator = new OperationGenerator(iterPrng);

      const res = SqliteProfileRunner.runIteration({
        seed,
        iteration: iterIndex,
        prng: iterPrng,
        generator: iterGenerator,
        invariantSuite,
      });

      accumulateStats(totals, res.stats);

      if (!res.success) {
        totals.violations++;
        totals.runtimeMs = Date.now() - startTime;
        totals.failureReport = ReproWriter.formatFailureReport({
          seed,
          iteration: iterIndex,
          profile: "sqlite",
          violatedInvariant: res.violatedInvariant,
          error: res.error,
          trace: res.trace,
          engine: res.engine,
          adapters: res.adapters,
          affectedAggregateId: res.affectedAggregateId,
        });
        ReproWriter.writeFailureArtifact({
          seed,
          iteration: iterIndex,
          profile: "sqlite",
          violatedInvariant: res.violatedInvariant,
          error: res.error,
          trace: res.trace,
        });
        return totals;
      }
    }

    totals.runtimeMs = Date.now() - startTime;
    totals.invariantCoverage = invariantSuite.getCounters();
    totals.invariantChecksCount = Object.values(totals.invariantCoverage).reduce((a, b) => a + b, 0);

    return totals;
  }
}

function accumulateStats(totals, stats) {
  if (!stats) return;
  totals.operationsExecuted += stats.operations || 0;
  totals.expectedDomainRejections += stats.domainRejections || 0;
  totals.injectedFailures += stats.failuresInjected || 0;
  totals.retries += stats.retries || 0;
  totals.recoveries += stats.recoveries || 0;
  totals.reopens += stats.reopens || 0;
}

module.exports = {
  ChaosRunner,
  CAMPAIGNS,
};
