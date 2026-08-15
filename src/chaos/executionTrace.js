class ExecutionTrace {
  #seed;
  #iteration;
  #profile;
  #steps;

  constructor({ seed, iteration, profile = "memory" }) {
    this.#seed = seed;
    this.#iteration = iteration;
    this.#profile = profile;
    this.#steps = [];
  }

  get seed() {
    return this.#seed;
  }

  get iteration() {
    return this.#iteration;
  }

  get profile() {
    return this.#profile;
  }

  recordStep({ opName, args = {}, outcome = {}, classification = "SUCCESS", invariantsChecked = [] }) {
    const step = {
      stepIndex: this.#steps.length + 1,
      timestamp: new Date().toISOString(),
      opName,
      args: structuredCloneSafe(args),
      outcome: {
        success: Boolean(outcome.success),
        resultSummary: outcome.resultSummary || (outcome.result ? summarizeResult(outcome.result) : null),
        error: outcome.error
          ? {
              code: outcome.error.code || outcome.error.name || "Error",
              message: outcome.error.message,
            }
          : null,
      },
      classification,
      invariantsChecked: [...invariantsChecked],
    };

    this.#steps.push(step);
    return step;
  }

  getSteps() {
    return [...this.#steps];
  }

  formatCompact() {
    if (this.#steps.length === 0) return "  (no operations executed)";
    return this.#steps
      .map((s) => {
        const idx = String(s.stepIndex).padStart(2, "0");
        const argsStr = Object.entries(s.args || {})
          .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`)
          .join(" ");
        const statusStr = s.outcome.success
          ? "OK"
          : `ERR [${s.outcome.error?.code || s.classification}]`;
        return `  ${idx} ${s.opName.padEnd(26)} ${statusStr.padEnd(16)} ${argsStr}`;
      })
      .join("\n");
  }

  toJSON() {
    return {
      seed: this.#seed,
      iteration: this.#iteration,
      profile: this.#profile,
      stepsCount: this.#steps.length,
      steps: this.#steps,
    };
  }
}

function summarizeResult(res) {
  if (!res || typeof res !== "object") return String(res);
  const summary = {};
  if (res.aggregateId !== undefined) summary.aggregateId = res.aggregateId;
  if (res.status !== undefined) summary.status = res.status;
  if (res.lifecycle !== undefined) summary.lifecycle = res.lifecycle;
  if (res.version !== undefined) summary.version = res.version;
  if (res.events?.length !== undefined) summary.eventsCount = res.events.length;
  return Object.keys(summary).length > 0 ? summary : "[Object]";
}

function structuredCloneSafe(val) {
  if (val === undefined || val === null) return val;
  try {
    return structuredClone(val);
  } catch {
    return JSON.parse(JSON.stringify(val));
  }
}

module.exports = {
  ExecutionTrace,
};
