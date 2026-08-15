const test = require("node:test");
const assert = require("node:assert/strict");
const { once } = require("node:events");

const { createApp } = require("../src/app");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { DIAGNOSTIC_TYPES, DIAGNOSTIC_STATUSES } = require("../src/application/diagnostics");

async function createTestApi() {
  const rollbackEngine = new RollbackEngine();
  const app = createApp({ rollbackEngine });
  const server = app.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;

  return {
    baseUrl,
    rollbackEngine,
    async close() {
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

test("GET /order/:id returns single order with consistency mode", async () => {
  const api = await createTestApi();

  try {
    const checkout = api.rollbackEngine.checkout({
      item: "Screen",
      quantity: 1,
      amount: 400,
    });

    const resMat = await fetch(`${api.baseUrl}/order/${checkout.aggregateId}`);
    assert.equal(resMat.status, 200);
    const bodyMat = await resMat.json();
    assert.equal(bodyMat.id, checkout.aggregateId);
    assert.equal(bodyMat.item, "Screen");

    const resAuth = await fetch(`${api.baseUrl}/order/${checkout.aggregateId}?consistency=authoritative`);
    assert.equal(resAuth.status, 200);
    const bodyAuth = await resAuth.json();
    assert.equal(bodyAuth.item, "Screen");

    const resNotFound = await fetch(`${api.baseUrl}/order/9999`);
    assert.equal(resNotFound.status, 404);

    const resInvalidConsistency = await fetch(`${api.baseUrl}/order/${checkout.aggregateId}?consistency=invalid`);
    assert.equal(resInvalidConsistency.status, 400);
  } finally {
    await api.close();
  }
});

test("GET /state-at/:orderId/sequence/:sequence returns exact point-in-time projection", async () => {
  const api = await createTestApi();

  try {
    const checkout = api.rollbackEngine.checkout({
      item: "Tablet",
      quantity: 1,
      amount: 300,
    });

    const resSeq1 = await fetch(`${api.baseUrl}/state-at/${checkout.aggregateId}/sequence/1`);
    assert.equal(resSeq1.status, 200);
    const stateSeq1 = await resSeq1.json();
    assert.equal(stateSeq1.version, 1);
    assert.equal(stateSeq1.order.status, "created");
    assert.equal(stateSeq1.inventory, null);

    const resSeq2 = await fetch(`${api.baseUrl}/state-at/${checkout.aggregateId}/sequence/2`);
    assert.equal(resSeq2.status, 200);
    const stateSeq2 = await resSeq2.json();
    assert.equal(stateSeq2.version, 2);
    assert.equal(stateSeq2.inventory.status, "reserved");
    assert.equal(stateSeq2.payment, null);

    const resSeq3 = await fetch(`${api.baseUrl}/state-at/${checkout.aggregateId}/sequence/3`);
    assert.equal(resSeq3.status, 200);
    const stateSeq3 = await resSeq3.json();
    assert.equal(stateSeq3.version, 3);
    assert.equal(stateSeq3.payment.status, "charged");

    const resNotFound = await fetch(`${api.baseUrl}/state-at/8888/sequence/1`);
    assert.equal(resNotFound.status, 404);
  } finally {
    await api.close();
  }
});

test("GET /diagnostics returns diagnostic events with filters", async () => {
  const api = await createTestApi();

  try {
    const key = "api-diag-key-1";
    await fetch(`${api.baseUrl}/checkout`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": key,
      },
      body: JSON.stringify({ item: "Chair", quantity: 1, amount: 120 }),
    });

    // Idempotent retry triggers deduplication diagnostic
    await fetch(`${api.baseUrl}/checkout`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": key,
      },
      body: JSON.stringify({ item: "Chair", quantity: 1, amount: 120 }),
    });

    const resDiag = await fetch(`${api.baseUrl}/diagnostics?type=IDEMPOTENCY_DEDUPLICATED`);
    assert.equal(resDiag.status, 200);
    const diagBody = await resDiag.json();
    assert.equal(diagBody.count >= 1, true);
    assert.equal(diagBody.diagnostics[0].commandId, key);
    assert.equal(diagBody.diagnostics[0].type, "IDEMPOTENCY_DEDUPLICATED");
  } finally {
    await api.close();
  }
});
