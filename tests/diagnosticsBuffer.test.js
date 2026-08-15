const test = require("node:test");
const assert = require("node:assert/strict");

const {
  createDiagnosticEmitter,
  DIAGNOSTIC_TYPES,
  DIAGNOSTIC_STATUSES,
} = require("../src/application/diagnostics");
const { RollbackEngine } = require("../src/application/rollbackEngine");

test("createDiagnosticEmitter records diagnostics in queryable buffer", () => {
  const emitter = createDiagnosticEmitter();

  emitter({
    type: DIAGNOSTIC_TYPES.EVENT_APPEND,
    status: DIAGNOSTIC_STATUSES.COMMIT_UNKNOWN,
    aggregateId: 1,
    commandId: "cmd-1",
  });

  emitter({
    type: DIAGNOSTIC_TYPES.SNAPSHOT_SAVE,
    status: DIAGNOSTIC_STATUSES.SAVE_FAILED,
    aggregateId: 2,
    commandId: "cmd-2",
  });

  emitter({
    type: DIAGNOSTIC_TYPES.IDEMPOTENCY_DEDUPLICATED,
    status: DIAGNOSTIC_STATUSES.DEDUPLICATED,
    aggregateId: 1,
    commandId: "cmd-1",
  });

  const all = emitter.query();
  assert.equal(all.length, 3);

  const filterType = emitter.query({ type: DIAGNOSTIC_TYPES.SNAPSHOT_SAVE });
  assert.equal(filterType.length, 1);
  assert.equal(filterType[0].aggregateId, 2);

  const filterAgg = emitter.query({ aggregateId: 1 });
  assert.equal(filterAgg.length, 2);

  const filterCmd = emitter.query({ commandId: "cmd-2" });
  assert.equal(filterCmd.length, 1);

  const filterLimit = emitter.query({ limit: 1 });
  assert.equal(filterLimit.length, 1);
  assert.equal(filterLimit[0].type, DIAGNOSTIC_TYPES.IDEMPOTENCY_DEDUPLICATED);
});

test("RollbackEngine records idempotency deduplication diagnostics", () => {
  const engine = new RollbackEngine();

  const command = { item: "Headphones", quantity: 1, amount: 80 };
  const key = "idemp-diag-test-1";

  engine.checkout(command, { commandId: key });
  engine.checkout(command, { commandId: key });

  const diagnostics = engine.getDiagnostics({
    type: DIAGNOSTIC_TYPES.IDEMPOTENCY_DEDUPLICATED,
  });

  assert.equal(diagnostics.length, 1);
  assert.equal(diagnostics[0].commandId, key);
  assert.equal(diagnostics[0].status, DIAGNOSTIC_STATUSES.DEDUPLICATED);
});
