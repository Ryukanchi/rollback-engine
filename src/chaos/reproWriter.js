const fs = require("node:fs");
const path = require("node:path");

class ReproWriter {
  static formatFailureReport({
    seed,
    iteration,
    profile,
    violatedInvariant,
    error,
    trace,
    engine,
    adapters,
    affectedAggregateId,
  }) {
    const lines = [];

    lines.push("============================================================");
    lines.push("           CHAOS INVARIANT VIOLATION DETECTED               ");
    lines.push("============================================================");
    lines.push(`Seed:        ${seed}`);
    lines.push(`Iteration:   ${iteration}`);
    lines.push(`Profile:     ${profile}`);
    lines.push(`Invariant:   ${violatedInvariant}`);
    lines.push(`Message:     ${error.message}`);
    lines.push("------------------------------------------------------------");
    lines.push("REPRODUCTION COMMAND:");
    lines.push(`  npm run chaos -- --seed=${seed} --iteration=${iteration} --profile=${profile}`);
    lines.push("------------------------------------------------------------");
    lines.push("OPERATION TRACE:");
    lines.push(trace.formatCompact());
    lines.push("------------------------------------------------------------");

    if (error.details && Object.keys(error.details).length > 0) {
      lines.push("VIOLATION DETAILS:");
      lines.push(JSON.stringify(error.details, null, 2));
      lines.push("------------------------------------------------------------");
    }

    if (affectedAggregateId && adapters?.eventStore) {
      const events = adapters.eventStore.getByAggregateId(affectedAggregateId);
      lines.push(`EVENT TIMELINE (Aggregate ${affectedAggregateId}, ${events.length} events):`);
      if (events.length === 0) {
        lines.push("  (no events committed for this aggregate)");
      } else {
        events.forEach((e) => {
          lines.push(`  #${e.sequence} [${e.eventType}] cmd=${e.metadata?.commandId || "-"}`);
        });
      }
      lines.push("------------------------------------------------------------");
    }

    if (engine && adapters?.stateRepository && affectedAggregateId) {
      try {
        const replay = engine.replay(affectedAggregateId);
        const materialized = engine.getState(affectedAggregateId, { consistency: "materialized" });
        lines.push("STATE SNAPSHOT:");
        lines.push(`  Replay State:       ${JSON.stringify(replay)}`);
        lines.push(`  Materialized State: ${JSON.stringify(materialized)}`);
      } catch {
        // Ignore state inspection errors during report printing
      }
      lines.push("------------------------------------------------------------");
    }

    lines.push("============================================================");
    return lines.join("\n");
  }

  static writeFailureArtifact({ artifactDir = "artifacts/chaos-failures", ...reportData }) {
    try {
      if (!fs.existsSync(artifactDir)) {
        fs.mkdirSync(artifactDir, { recursive: true });
      }

      const fileName = `failure-seed${reportData.seed}-iter${reportData.iteration}.json`;
      const filePath = path.join(artifactDir, fileName);

      const artifact = {
        timestamp: new Date().toISOString(),
        seed: reportData.seed,
        iteration: reportData.iteration,
        profile: reportData.profile,
        violatedInvariant: reportData.violatedInvariant,
        errorMessage: reportData.error?.message,
        errorDetails: reportData.error?.details,
        trace: reportData.trace?.toJSON(),
        reproductionCommand: `npm run chaos -- --seed=${reportData.seed} --iteration=${reportData.iteration} --profile=${reportData.profile}`,
      };

      fs.writeFileSync(filePath, JSON.stringify(artifact, null, 2), "utf8");
      return filePath;
    } catch {
      return null;
    }
  }
}

module.exports = {
  ReproWriter,
};
