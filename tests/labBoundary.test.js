const test = require("node:test");
const assert = require("node:assert/strict");
const { createApp } = require("../src/app");

async function createServer(app) {
  return new Promise((resolve) => {
    const server = app.listen(0, () => {
      const port = server.address().port;
      resolve({
        baseUrl: `http://localhost:${port}`,
        close: () => new Promise((res) => server.close(res)),
      });
    });
  });
}

test("when LAB_MODE is disabled, /lab and lab API routes return 404 while normal API remains functional", async () => {
  const app = createApp({ labMode: false });
  const server = await createServer(app);

  try {
    // Lab routes must be disabled
    const labRes = await fetch(`${server.baseUrl}/lab`);
    assert.equal(labRes.status, 404);

    const labApiRes = await fetch(`${server.baseUrl}/lab/api/scenarios/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenarioType: "successful_checkout" }),
    });
    assert.equal(labApiRes.status, 404);

    // Normal API routes must continue to work
    const rootRes = await fetch(`${server.baseUrl}/`);
    assert.equal(rootRes.status, 200);

    const openApiRes = await fetch(`${server.baseUrl}/openapi.json`);
    assert.equal(openApiRes.status, 200);

    const checkoutRes = await fetch(`${server.baseUrl}/checkout`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ item: "TestItem", quantity: 1, amount: 100 }),
    });
    assert.equal(checkoutRes.status, 201);
  } finally {
    await server.close();
  }
});

test("when LAB_MODE is enabled, /lab UI and API routes are accessible", async () => {
  const app = createApp({ labMode: true });
  const server = await createServer(app);

  try {
    // Health endpoint
    const healthRes = await fetch(`${server.baseUrl}/lab/api/health`);
    assert.equal(healthRes.status, 200);
    const health = await healthRes.json();
    assert.equal(health.status, "ok");
    assert.equal(health.labMode, true);

    // Static HTML
    const labRes = await fetch(`${server.baseUrl}/lab/`);
    assert.equal(labRes.status, 200);
    const html = await labRes.text();
    assert.equal(html.includes("Rollback Engine Lab"), true);

    // Static CSS and JS
    const cssRes = await fetch(`${server.baseUrl}/lab/styles.css`);
    assert.equal(cssRes.status, 200);

    const jsRes = await fetch(`${server.baseUrl}/lab/app.js`);
    assert.equal(jsRes.status, 200);

    // Scenario Run Endpoint
    const scenarioRes = await fetch(`${server.baseUrl}/lab/api/scenarios/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenarioType: "read_model_drift" }),
    });
    assert.equal(scenarioRes.status, 200);
    const scenarioData = await scenarioRes.json();
    assert.equal(scenarioData.status, "drift_detected");
    const scenarioId = scenarioData.scenarioId;

    // GET /lab/api/scenarios/:id
    const getRes = await fetch(`${server.baseUrl}/lab/api/scenarios/${scenarioId}`);
    assert.equal(getRes.status, 200);
    const getData = await getRes.json();
    assert.equal(getData.scenarioId, scenarioId);

    // GET /lab/api/scenarios/:id/state/:sequence
    const seqRes = await fetch(`${server.baseUrl}/lab/api/scenarios/${scenarioId}/state/2`);
    assert.equal(seqRes.status, 200);
    const seqData = await seqRes.json();
    assert.equal(seqData.sequence, 2);

    // POST /lab/api/scenarios/:id/repair
    const repairRes = await fetch(`${server.baseUrl}/lab/api/scenarios/${scenarioId}/repair`, {
      method: "POST",
    });
    assert.equal(repairRes.status, 200);
    const repairData = await repairRes.json();
    assert.equal(repairData.status, "repaired");

    // DELETE /lab/api/scenarios/:id
    const deleteRes = await fetch(`${server.baseUrl}/lab/api/scenarios/${scenarioId}`, {
      method: "DELETE",
    });
    assert.equal(deleteRes.status, 200);
    const deleteData = await deleteRes.json();
    assert.equal(deleteData.closed, true);

    // 404 after delete
    const notFoundRes = await fetch(`${server.baseUrl}/lab/api/scenarios/${scenarioId}`);
    assert.equal(notFoundRes.status, 404);
  } finally {
    await server.close();
  }
});
