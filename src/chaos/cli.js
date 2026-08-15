#!/usr/bin/env node
const { ChaosRunner } = require("./chaosRunner");

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      const [key, val] = arg.slice(2).split("=");
      if (val !== undefined) {
        args[key] = isNaN(val) ? val : Number(val);
      } else {
        const next = argv[i + 1];
        if (next && !next.startsWith("--")) {
          args[key] = isNaN(next) ? next : Number(next);
          i++;
        } else {
          args[key] = true;
        }
      }
    }
  }
  return args;
}

function main() {
  const args = parseArgs(process.argv);
  const seed = args.seed !== undefined ? args.seed : (Date.now() ^ (Math.random() * 0x100000000)) >>> 0;

  console.log("============================================================");
  console.log("   ROLLBACK ENGINE — DETERMINISTIC CHAOS & INVARIANT FUZZER  ");
  console.log("============================================================");
  console.log(`Seed:       ${seed}`);
  console.log(`Campaign:   ${args.campaign || (args.iterations ? "custom" : "standard")}`);
  console.log(`Profile:    ${args.profile || "all"}`);
  if (args.iteration !== undefined) {
    console.log(`Single Iter: ${args.iteration} (Reproduction Mode)`);
  }
  console.log("------------------------------------------------------------");

  const result = ChaosRunner.runCampaign({
    seed,
    campaign: args.campaign,
    iterations: args.iterations,
    profile: args.profile,
    iteration: args.iteration,
  });

  if (result.violations > 0) {
    console.error(result.failureReport);
    console.error("\nRESULT: FAIL (Invariant Violation Detected)\n");
    process.exit(1);
  }

  const opsPerSec = result.runtimeMs > 0
    ? Math.round((result.operationsExecuted / (result.runtimeMs / 1000)))
    : result.operationsExecuted;

  console.log("\n--- CAMPAIGN EXECUTION SUMMARY ---");
  console.log(`Total Iterations:             ${result.totalIterations}`);
  console.log(`  - In-Memory Iterations:     ${result.memoryIterations}`);
  console.log(`  - SQLite Iterations:        ${result.sqliteIterations}`);
  console.log(`Operations Executed:          ${result.operationsExecuted} (~${opsPerSec} ops/sec)`);
  console.log(`Expected Domain Rejections:   ${result.expectedDomainRejections}`);
  console.log(`Injected Failures:            ${result.injectedFailures}`);
  console.log(`Command Retries Executed:     ${result.retries}`);
  console.log(`Recoveries / Self-Heals:      ${result.recoveries}`);
  if (result.reopens > 0) {
    console.log(`SQLite Database Reopens:      ${result.reopens}`);
  }
  console.log(`Total Invariant Checks:       ${result.invariantChecksCount}`);
  console.log(`Invariant Violations:         0`);
  console.log(`Execution Runtime:            ${result.runtimeMs} ms`);

  if (result.invariantCoverage) {
    console.log("\n--- INVARIANT COVERAGE REPORT ---");
    for (const [inv, count] of Object.entries(result.invariantCoverage)) {
      console.log(`  ${inv.padEnd(32)} ${String(count).padStart(8)} checks`);
    }
  }

  if (result.warnings && result.warnings.length > 0) {
    console.log("\n--- COVERAGE WARNINGS ---");
    for (const w of result.warnings) {
      console.warn(`  ⚠ ${w}`);
    }
  }

  console.log("\nRESULT: PASS (All Invariants 100% Satisfied)\n");
}

if (require.main === module) {
  main();
}
