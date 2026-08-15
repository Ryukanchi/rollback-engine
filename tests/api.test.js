const test = require("node:test");
const assert = require("node:assert/strict");
const { once } = require("node:events");

const { createApp } = require("../src/app");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { EVENT_TYPES, createDomainEvent } = require("../src/domain/events");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const {
  InMemorySnapshotStore,
} = require("../src/infrastructure/inMemorySnapshotStore");
const {
  InMemoryStateRepository,
} = require("../src/infrastructure/inMemoryStateRepository");

async function createApiHarness({
  eventStore = new InMemoryEventStore(),
  stateRepository = new InMemoryStateRepository(),
  snapshotStore = new InMemorySnapshotStore(),
  clock,
  diagnosticReporter,
} = {}) {
  let eventId = 0;
  let timestamp = 0;
  const effectiveClock =
    clock ??
    (() => new Date(Date.UTC(2026, 7, 15, 10, 0, timestamp++)).toISOString());
  const rollbackEngine = new RollbackEngine({
    eventStore,
    stateRepository,
    snapshotStore,
    eventIdGenerator: () => `api-event-${++eventId}`,
    clock: effectiveClock,
    diagnosticReporter,
  });
  const app = createApp({ rollbackEngine });
  const server = app.listen(0, "127.0.0.1");

  await once(server, "listening");

  const address = server.address();

  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    eventStore,
    rollbackEngine,
    stateRepository,
    async close() {
      await new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
  };
}

async function requestJson(baseUrl, path, options = {}) {
  const headers = { ...options.headers };
  let body = options.body;

  if (body !== undefined && typeof body !== "string") {
    headers["content-type"] = "application/json";
    body = JSON.stringify(body);
  }

  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers,
    body,
  });
  const text = await response.text();

  return {
    response,
    body: text ? JSON.parse(text) : null,
  };
}

async function withApi(run, options) {
  const harness = await createApiHarness(options);

  try {
    await run(harness);
  } finally {
    await harness.close();
  }
}

test("GET / exposes the migrated application", async () => {
  await withApi(async ({ baseUrl }) => {
    const response = await fetch(`${baseUrl}/`);

    assert.equal(response.status, 200);
    assert.equal(await response.text(), "Rollback Engine läuft 😈");
  });
});

test("GET /openapi.json exposes the complete public contract", async () => {
  await withApi(async ({ baseUrl }) => {
    const contract = await requestJson(baseUrl, "/openapi.json");
    const expectedPaths = [
      "/",
      "/checkout",
      "/history",
      "/openapi.json",
      "/order",
      "/order/{id}",
      "/orders",
      "/replay-restore/{orderId}",
      "/replay-state/{orderId}",
      "/snapshot/{orderId}",
      "/state-at/{orderId}/{timestamp}",
      "/timeline/{orderId}",
    ];

    assert.equal(contract.response.status, 200);
    assert.equal(contract.body.openapi, "3.1.0");
    assert.deepEqual(Object.keys(contract.body.paths).sort(), expectedPaths);
    assert.deepEqual(
      contract.body.components.schemas.ErrorDetail.required,
      [
        "code",
        "category",
        "message",
        "eventCommitted",
        "retrySafe",
        "retryAction",
      ]
    );
    assert.deepEqual(
      contract.body.components.schemas.ErrorDetail.properties.eventCommitted.type,
      ["boolean", "null"]
    );
    assert.equal(
      contract.body.components.responses.TechnicalError.headers[
        "Idempotency-Key"
      ].$ref,
      "#/components/headers/IdempotencyKey"
    );
    assert.deepEqual(
      contract.body.paths["/checkout"].post.parameters.map(
        (parameter) => parameter.$ref
      ),
      [
        "#/components/parameters/IdempotencyKey",
        "#/components/parameters/CorrelationId",
        "#/components/parameters/CausationId",
      ]
    );
    const completedExample =
      contract.body.paths["/checkout"].post.responses["201"].content[
        "application/json"
      ].examples.completed.value;
    const compensatedExample =
      contract.body.paths["/checkout"].post.responses["500"].content[
        "application/json"
      ].examples.compensated.value;

    assert.deepEqual(
      completedExample.events.map((event) => event.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
      ]
    );
    assert.deepEqual(
      compensatedExample.events.map((event) => event.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
        EVENT_TYPES.PAYMENT_REFUNDED,
        EVENT_TYPES.INVENTORY_RELEASED,
        EVENT_TYPES.ORDER_ROLLED_BACK,
      ]
    );
    assert.equal(completedExample.snapshot.state.version, 3);
    assert.equal(compensatedExample.snapshot.state.version, 6);
    assert.deepEqual(
      compensatedExample.events.slice(1).map((event) => event.metadata.causationId),
      compensatedExample.events.slice(0, -1).map((event) => event.eventId)
    );
    assert.equal(
      contract.body.paths["/checkout"].post.responses["500"].content[
        "application/json"
      ].examples.unknownCommit.value.error.eventCommitted,
      null
    );
  });
});

test("POST /checkout completes successfully and exposes history, replay and snapshot", async () => {
  await withApi(async ({ baseUrl }) => {
    const checkout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      body: {
        item: "Pizza",
        quantity: 1,
        amount: 100,
      },
    });

    assert.equal(checkout.response.status, 201);
    assert.equal(checkout.body.status, "completed");
    assert.equal(checkout.body.state.lifecycle, "completed");
    assert.deepEqual(
      checkout.body.events.map((event) => event.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
      ]
    );

    const aggregateId = checkout.body.aggregateId;
    const replay = await requestJson(baseUrl, `/replay-state/${aggregateId}`);
    const history = await requestJson(baseUrl, "/history");
    const snapshot = await requestJson(baseUrl, `/snapshot/${aggregateId}`);

    assert.equal(replay.response.status, 200);
    assert.deepEqual(replay.body, checkout.body.state);
    assert.equal(history.response.status, 200);
    assert.equal(history.body.count, 3);
    assert.deepEqual(
      history.body.history.map((event) => event.sequence),
      [1, 2, 3]
    );
    assert.equal(snapshot.response.status, 200);
    assert.equal(snapshot.body.snapshot.version, 3);
    assert.deepEqual(snapshot.body.snapshot.state, checkout.body.state);
  });
});

test("snapshot persistence failures remain visible without changing command success", async () => {
  class UnavailableSnapshotStore extends InMemorySnapshotStore {
    save() {
      throw new Error("Snapshot store unavailable");
    }
  }

  await withApi(
    async ({ baseUrl, eventStore }) => {
      const checkout = await requestJson(baseUrl, "/checkout", {
        method: "POST",
        body: { item: "Pizza", quantity: 1, amount: 100 },
      });

      assert.equal(checkout.response.status, 201);
      assert.equal(checkout.body.status, "completed");
      assert.equal(checkout.body.snapshot, null);
      assert.deepEqual(checkout.body.warnings, [
        {
          code: "SNAPSHOT_SAVE_FAILED",
          category: "technical",
          message:
            "The command committed successfully, but its snapshot could not be saved.",
          eventCommitted: true,
          retrySafe: false,
          retryAction: "DO_NOT_RETRY_COMMAND",
          aggregateId: 1,
        },
      ]);

      const created = await requestJson(baseUrl, "/order", {
        method: "POST",
        body: { item: "Salad" },
      });
      const deleted = await requestJson(baseUrl, `/order/${created.body.id}`, {
        method: "DELETE",
      });

      assert.equal(deleted.response.status, 200);
      assert.deepEqual(deleted.body.warnings, [
        {
          code: "SNAPSHOT_SAVE_FAILED",
          category: "technical",
          message:
            "The command committed successfully, but its snapshot could not be saved.",
          eventCommitted: true,
          retrySafe: false,
          retryAction: "DO_NOT_RETRY_COMMAND",
          aggregateId: 2,
        },
      ]);
      assert.equal(eventStore.getAll().length, 5);
    },
    { snapshotStore: new UnavailableSnapshotStore() }
  );
});

test("POST /checkout persists a complete rollback after a simulated payment failure", async () => {
  await withApi(async ({ baseUrl }) => {
    const checkout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      body: {
        item: "Pizza",
        quantity: 1,
        amount: 100,
        simulateFailureAt: "after_payment",
      },
    });

    assert.equal(checkout.response.status, 500);
    assert.equal(checkout.body.status, "rolled_back");
    assert.equal(checkout.body.state.order.status, "rolled_back");
    assert.equal(checkout.body.state.inventory.status, "released");
    assert.equal(checkout.body.state.payment.status, "refunded");
    assert.deepEqual(
      checkout.body.events.map((event) => event.eventType),
      [
        EVENT_TYPES.ORDER_CREATED,
        EVENT_TYPES.INVENTORY_RESERVED,
        EVENT_TYPES.PAYMENT_CHARGED,
        EVENT_TYPES.PAYMENT_REFUNDED,
        EVENT_TYPES.INVENTORY_RELEASED,
        EVENT_TYPES.ORDER_ROLLED_BACK,
      ]
    );

    const replay = await requestJson(
      baseUrl,
      `/replay-state/${checkout.body.aggregateId}`
    );

    assert.deepEqual(replay.body, checkout.body.state);
  });
});

test("GET /timeline exposes an aggregate trace without creating events", async () => {
  await withApi(async ({ baseUrl, eventStore }) => {
    const checkout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      headers: {
        "Idempotency-Key": "timeline-command",
        "X-Correlation-Id": "timeline-flow",
        "X-Causation-Id": "timeline-request",
      },
      body: {
        item: "Pizza",
        quantity: 1,
        amount: 100,
        simulateFailureAt: "after_payment",
      },
    });
    const eventCount = eventStore.getAll().length;
    const timeline = await requestJson(
      baseUrl,
      `/timeline/${checkout.body.aggregateId}`
    );

    assert.equal(timeline.response.status, 200);
    assert.equal(timeline.body.aggregateId, checkout.body.aggregateId);
    assert.equal(timeline.body.version, 6);
    assert.equal(timeline.body.eventCount, 6);
    assert.deepEqual(timeline.body.commandIds, ["timeline-command"]);
    assert.deepEqual(timeline.body.correlationIds, ["timeline-flow"]);
    assert.deepEqual(
      timeline.body.entries.map((entry) => [entry.sequence, entry.eventType]),
      [
        [1, EVENT_TYPES.ORDER_CREATED],
        [2, EVENT_TYPES.INVENTORY_RESERVED],
        [3, EVENT_TYPES.PAYMENT_CHARGED],
        [4, EVENT_TYPES.PAYMENT_REFUNDED],
        [5, EVENT_TYPES.INVENTORY_RELEASED],
        [6, EVENT_TYPES.ORDER_ROLLED_BACK],
      ]
    );
    assert.equal(timeline.body.entries[0].causationId, "timeline-request");
    assert.equal(
      timeline.body.entries[1].causationId,
      timeline.body.entries[0].eventId
    );
    assert.equal(eventStore.getAll().length, eventCount);

    const missing = await requestJson(baseUrl, "/timeline/999");

    assert.equal(missing.response.status, 404);
    assert.equal(missing.body.error.code, "AGGREGATE_NOT_FOUND");
  });
});

test("POST /order and DELETE /order/:id use ORDER_CREATED and ORDER_DELETED", async () => {
  await withApi(async ({ baseUrl }) => {
    const created = await requestJson(baseUrl, "/order", {
      method: "POST",
      body: { item: "Pizza" },
    });

    assert.equal(created.response.status, 201);
    assert.equal(created.body.id, 1);
    assert.equal(created.body.status, "created");
    assert.equal(created.body.quantity, 1);

    const ordersBeforeDelete = await requestJson(baseUrl, "/orders");

    assert.equal(ordersBeforeDelete.response.status, 200);
    assert.deepEqual(ordersBeforeDelete.body, [created.body]);

    const deleted = await requestJson(baseUrl, "/order/1", {
      method: "DELETE",
    });

    assert.equal(deleted.response.status, 200);
    assert.equal(deleted.body.message, "Order deleted");
    assert.deepEqual(deleted.body.order, created.body);

    const ordersAfterDelete = await requestJson(baseUrl, "/orders");
    const replay = await requestJson(baseUrl, "/replay-state/1");
    const history = await requestJson(baseUrl, "/history");

    assert.deepEqual(ordersAfterDelete.body, []);
    assert.equal(replay.body.lifecycle, "deleted");
    assert.equal(replay.body.deleted, true);
    assert.equal(replay.body.order, null);
    assert.equal(replay.body.tombstone.aggregateId, 1);
    assert.deepEqual(
      history.body.history.map((event) => event.eventType),
      [EVENT_TYPES.ORDER_CREATED, EVENT_TYPES.ORDER_DELETED]
    );
  });
});

test("DELETE /order/:id cannot bypass checkout compensation", async () => {
  await withApi(async ({ baseUrl }) => {
    const checkout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      body: { item: "Pizza", quantity: 1, amount: 100 },
    });
    const aggregateId = checkout.body.aggregateId;
    const deleted = await requestJson(baseUrl, `/order/${aggregateId}`, {
      method: "DELETE",
    });
    const replay = await requestJson(baseUrl, `/replay-state/${aggregateId}`);
    const history = await requestJson(baseUrl, "/history");

    assert.equal(deleted.response.status, 409);
    assert.match(deleted.body.error.message, /must be compensated/);
    assert.equal(deleted.body.error.code, "COMPENSATION_REQUIRED");
    assert.equal(deleted.body.error.category, "domain");
    assert.equal(deleted.body.error.eventCommitted, false);
    assert.equal(deleted.body.error.retryAction, "COMPENSATE_THEN_RETRY");
    assert.equal(replay.body.lifecycle, "completed");
    assert.equal(replay.body.payment.status, "charged");
    assert.equal(history.body.count, 3);
  });
});

test("POST /replay-restore recovers the materialized state from retained events", async () => {
  await withApi(async ({
    baseUrl,
    eventStore,
    stateRepository,
  }) => {
    const checkout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      body: { item: "Pizza", quantity: 1, amount: 100 },
    });
    const aggregateId = checkout.body.aggregateId;
    const eventCount = eventStore.getAll().length;

    stateRepository.reset();

    const emptyOrders = await requestJson(baseUrl, "/orders");
    assert.deepEqual(emptyOrders.body, []);

    const restored = await requestJson(baseUrl, `/replay-restore/${aggregateId}`, {
      method: "POST",
    });

    assert.equal(restored.response.status, 200);
    assert.equal(restored.body.restored, true);
    assert.equal(restored.body.state.lifecycle, "completed");
    assert.equal(eventStore.getAll().length, eventCount);

    const recoveredOrders = await requestJson(baseUrl, "/orders");
    assert.equal(recoveredOrders.body.length, 1);
    assert.equal(recoveredOrders.body[0].item, "Pizza");
  });
});

test("GET /state-at reconstructs state at an event timestamp", async () => {
  await withApi(async ({ baseUrl }) => {
    const checkout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      body: { item: "Pizza", quantity: 1, amount: 100 },
    });
    const firstTimestamp = checkout.body.events[0].timestamp;
    const stateAt = await requestJson(
      baseUrl,
      `/state-at/${checkout.body.aggregateId}/${encodeURIComponent(firstTimestamp)}`
    );

    assert.equal(stateAt.response.status, 200);
    assert.equal(stateAt.body.version, 1);
    assert.equal(stateAt.body.order.status, "created");
    assert.equal(stateAt.body.inventory, null);
    assert.equal(stateAt.body.payment, null);
  });
});

test("central error handling returns JSON for validation, missing resources and routes", async () => {
  await withApi(async ({ baseUrl }) => {
    const invalidCheckout = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      body: { item: "", quantity: 0, amount: -1 },
    });
    const missingReplay = await requestJson(baseUrl, "/replay-state/999");
    const invalidOrderId = await requestJson(baseUrl, "/replay-state/not-a-number");
    const missingRoute = await requestJson(baseUrl, "/does-not-exist");

    assert.equal(invalidCheckout.response.status, 400);
    assert.match(invalidCheckout.body.error.message, /item/);
    assert.equal(invalidCheckout.body.error.code, "VALIDATION_ERROR");
    assert.equal(invalidCheckout.body.error.category, "validation");
    assert.equal(invalidCheckout.body.error.eventCommitted, false);
    assert.equal(invalidCheckout.body.error.retryAction, "FIX_REQUEST");
    assert.equal(missingReplay.response.status, 404);
    assert.match(missingReplay.body.error.message, /does not exist/);
    assert.equal(missingReplay.body.error.code, "AGGREGATE_NOT_FOUND");
    assert.equal(invalidOrderId.response.status, 400);
    assert.match(invalidOrderId.body.error.message, /orderId/);
    assert.equal(invalidOrderId.body.error.code, "VALIDATION_ERROR");
    assert.equal(missingRoute.response.status, 404);
    assert.match(missingRoute.body.error.message, /Route GET/);
    assert.equal(missingRoute.body.error.code, "NOT_FOUND");
  });
});

test("committed view-repair failures expose safe retry information", async () => {
  class UnavailableStateRepository extends InMemoryStateRepository {
    save() {
      throw new Error("Materialized view unavailable");
    }

    replace() {
      throw new Error("Materialized view unavailable");
    }
  }

  await withApi(
    async ({ baseUrl, eventStore }) => {
      const checkout = await requestJson(baseUrl, "/checkout", {
        method: "POST",
        body: { item: "Pizza", quantity: 1, amount: 100 },
      });

      assert.equal(checkout.response.status, 500);
      assert.deepEqual(checkout.body, {
        error: {
          code: "EVENT_COMMITTED_VIEW_REPAIR_FAILED",
          category: "technical",
          message: "The event was committed, but the materialized view could not be repaired.",
          eventCommitted: true,
          retrySafe: false,
          retryAction: "MANUAL_RESOLUTION_REQUIRED",
          eventId: "api-event-1",
          eventIds: ["api-event-1"],
          aggregateId: 1,
        },
      });
      assert.equal(eventStore.getAll().length, 1);
    },
    { stateRepository: new UnavailableStateRepository() }
  );
});

test("known unkeyed partial commits expose their committed event IDs", async () => {
  let clockCalls = 0;
  const clock = () => {
    clockCalls += 1;

    if (clockCalls === 2) {
      throw new Error("Clock unavailable");
    }

    return "2026-08-15T10:00:00.000Z";
  };

  await withApi(
    async ({ baseUrl, eventStore }) => {
      const checkout = await requestJson(baseUrl, "/checkout", {
        method: "POST",
        body: { item: "Pizza", quantity: 1, amount: 100 },
      });

      assert.equal(checkout.response.status, 500);
      assert.deepEqual(checkout.body, {
        error: {
          code: "COMMAND_EXECUTION_PARTIALLY_COMMITTED",
          category: "technical",
          message:
            "The command committed one or more events but did not complete.",
          eventCommitted: true,
          retrySafe: false,
          retryAction: "MANUAL_RESOLUTION_REQUIRED",
          aggregateId: 1,
          eventIds: ["api-event-1"],
        },
      });
      assert.equal(eventStore.getAll().length, 1);
    },
    { clock }
  );
});

test("Idempotency-Key returns the original checkout response without new events", async () => {
  await withApi(async ({ baseUrl, eventStore }) => {
    const request = {
      method: "POST",
      headers: {
        "Idempotency-Key": "checkout-command-1",
        "X-Correlation-Id": "checkout-flow-1",
        "X-Causation-Id": "http-request-1",
      },
      body: { item: "Pizza", quantity: 1, amount: 100 },
    };

    const first = await requestJson(baseUrl, "/checkout", request);
    const repeated = await requestJson(baseUrl, "/checkout", request);

    assert.equal(first.response.status, 201);
    assert.equal(repeated.response.status, 201);
    assert.equal(repeated.response.headers.get("idempotency-key"), "checkout-command-1");
    assert.deepEqual(repeated.body, first.body);
    assert.equal(eventStore.getAll().length, 3);
    assert.deepEqual(eventStore.getAll()[0].metadata, {
      schemaVersion: 1,
      commandId: "checkout-command-1",
      correlationId: "checkout-flow-1",
      causationId: "http-request-1",
    });
    assert.equal(
      eventStore.getAll()[1].metadata.causationId,
      eventStore.getAll()[0].eventId
    );
  });
});

test("Idempotency-Key rejects a different checkout payload without new events", async () => {
  await withApi(async ({ baseUrl, eventStore }) => {
    const headers = { "Idempotency-Key": "checkout-command-1" };

    await requestJson(baseUrl, "/checkout", {
      method: "POST",
      headers,
      body: { item: "Pizza", quantity: 1, amount: 100 },
    });
    const conflict = await requestJson(baseUrl, "/checkout", {
      method: "POST",
      headers,
      body: { item: "Pasta", quantity: 1, amount: 100 },
    });

    assert.equal(conflict.response.status, 409);
    assert.deepEqual(conflict.body, {
      error: {
        code: "IDEMPOTENCY_KEY_CONFLICT",
        category: "conflict",
        message: "The idempotency key was already used for a different command.",
        eventCommitted: false,
        retrySafe: false,
        retryAction: "USE_NEW_KEY",
        commandId: "checkout-command-1",
      },
    });
    assert.equal(eventStore.getAll().length, 3);
  });
});

test("a concurrent aggregate write is retryable and never duplicates the command", async () => {
  class ConcurrentWriteEventStore extends InMemoryEventStore {
    injectConcurrentWrite = true;

    append(event, options) {
      if (this.injectConcurrentWrite) {
        this.injectConcurrentWrite = false;
        super.append(
          createDomainEvent({
            eventId: "concurrent-event-1",
            eventType: EVENT_TYPES.ORDER_CREATED,
            aggregateId: event.aggregateId,
            sequence: 1,
            timestamp: event.timestamp,
            payload: { item: "Concurrent order", quantity: 1 },
            metadata: {
              schemaVersion: 1,
              commandId: "concurrent-command",
              correlationId: "concurrent-command",
              causationId: "concurrent-command",
            },
          }),
          { expectedVersion: 0 }
        );
      }

      return super.append(event, options);
    }
  }

  await withApi(
    async ({ baseUrl, eventStore }) => {
      const request = {
        method: "POST",
        headers: { "Idempotency-Key": "concurrent-create-command" },
        body: { item: "Pizza", quantity: 1 },
      };

      const conflict = await requestJson(baseUrl, "/order", request);

      assert.equal(conflict.response.status, 409);
      assert.deepEqual(conflict.body, {
        error: {
          code: "OPTIMISTIC_CONCURRENCY_CONFLICT",
          category: "conflict",
          message: "The aggregate changed before the event could be committed.",
          eventCommitted: false,
          retrySafe: true,
          retryAction: "RETRY_SAME_KEY",
          commandId: "concurrent-create-command",
          aggregateId: 1,
        },
      });
      assert.deepEqual(
        eventStore.getByCommandId("concurrent-create-command"),
        []
      );

      const retried = await requestJson(baseUrl, "/order", request);

      assert.equal(retried.response.status, 201);
      assert.equal(retried.body.id, 2);
      assert.equal(
        eventStore.getByCommandId("concurrent-create-command").length,
        1
      );
      assert.equal(eventStore.getAll().length, 2);
    },
    { eventStore: new ConcurrentWriteEventStore() }
  );
});

test("a keyed partial commit returns the same non-retryable API error", async () => {
  let clockCalls = 0;
  const clock = () => {
    clockCalls += 1;

    if (clockCalls === 2) {
      throw new Error("Clock unavailable");
    }

    return "2026-08-15T10:00:00.000Z";
  };

  await withApi(
    async ({ baseUrl, eventStore }) => {
      const request = {
        method: "POST",
        headers: { "Idempotency-Key": "partial-checkout-command-1" },
        body: { item: "Pizza", quantity: 1, amount: 100 },
      };

      const first = await requestJson(baseUrl, "/checkout", request);
      const repeated = await requestJson(baseUrl, "/checkout", request);
      const expectedBody = {
        error: {
          code: "COMMAND_EXECUTION_PARTIALLY_COMMITTED",
          category: "technical",
          message:
            "The command committed one or more events but did not complete.",
          eventCommitted: true,
          retrySafe: false,
          retryAction: "MANUAL_RESOLUTION_REQUIRED",
          commandId: "partial-checkout-command-1",
          aggregateId: 1,
          eventIds: ["api-event-1"],
        },
      };

      assert.equal(first.response.status, 500);
      assert.equal(repeated.response.status, 500);
      assert.deepEqual(first.body, expectedBody);
      assert.deepEqual(repeated.body, expectedBody);
      assert.equal(eventStore.getAll().length, 1);
      assert.equal(clockCalls, 2);
    },
    { clock }
  );
});

test("order creation and deletion are idempotent across their HTTP routes", async () => {
  await withApi(async ({ baseUrl, eventStore }) => {
    const createRequest = {
      method: "POST",
      headers: { "Idempotency-Key": "create-order-command-1" },
      body: { item: "Pizza", quantity: 1 },
    };

    const created = await requestJson(baseUrl, "/order", createRequest);
    const repeatedCreate = await requestJson(baseUrl, "/order", createRequest);
    const deleteRequest = {
      method: "DELETE",
      headers: { "Idempotency-Key": "delete-order-command-1" },
    };
    const deleted = await requestJson(baseUrl, "/order/1", deleteRequest);
    const repeatedDelete = await requestJson(
      baseUrl,
      "/order/1",
      deleteRequest
    );

    assert.equal(created.response.status, 201);
    assert.deepEqual(repeatedCreate.body, created.body);
    assert.equal(deleted.response.status, 200);
    assert.deepEqual(repeatedDelete.body, deleted.body);
    assert.equal(eventStore.getAll().length, 2);
  });
});
