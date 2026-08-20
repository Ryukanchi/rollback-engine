const test = require("node:test");
const assert = require("node:assert/strict");

const { EventUpcasterRegistry } = require("../src/domain/eventUpcaster");
const { createDomainEvent, EVENT_TYPES } = require("../src/domain/events");
const { RollbackEngine } = require("../src/application/rollbackEngine");
const { InMemoryEventStore } = require("../src/infrastructure/inMemoryEventStore");
const { SqliteEventStore } = require("../src/infrastructure/sqlite/sqliteEventStore");
const { createSqliteDatabase } = require("../src/infrastructure/sqlite/sqliteDatabase");

const FIXED_TIMESTAMP = "2026-08-20T10:00:00.000Z";

function createHistoricalOrderEvent(overrides = {}) {
  return createDomainEvent({
    eventId: "event-v1",
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 42,
    sequence: 1,
    timestamp: FIXED_TIMESTAMP,
    payload: { item: "Book", quantity: 2 },
    metadata: {
      schemaVersion: 1,
      commandId: "command-v1",
      correlationId: "correlation-v1",
      causationId: "causation-v1",
    },
    ...overrides,
  });
}

function registerOrderUpcaster(registry, upcast, versions = {}) {
  return registry.register({
    eventType: EVENT_TYPES.ORDER_CREATED,
    fromVersion: versions.fromVersion ?? 1,
    toVersion: versions.toVersion ?? 2,
    upcast,
  });
}

function assertUpcastError(code, reason) {
  return (error) =>
    error.code === code &&
    (reason === undefined || error.reason === reason);
}

function createEventStore(adapterName, upcasterRegistry) {
  if (adapterName === "memory") {
    return {
      eventStore: new InMemoryEventStore({ upcasterRegistry }),
      close: () => {},
    };
  }

  const db = createSqliteDatabase({ path: ":memory:" });
  return {
    eventStore: new SqliteEventStore({ db, upcasterRegistry }),
    close: () => db.close(),
  };
}

function createChangingIdentityOutput(
  event,
  fieldName,
  forgedValue,
  readCounters = []
) {
  const output = {
    ...event,
    payload: { ...event.payload, currency: "USD" },
  };
  const counter = { fieldName, reads: 0 };
  readCounters.push(counter);
  const readOnceThenForge = (stableValue) => {
    counter.reads += 1;
    return counter.reads === 1 ? stableValue : forgedValue;
  };

  if (["eventId", "aggregateId", "sequence"].includes(fieldName)) {
    Object.defineProperty(output, fieldName, {
      enumerable: true,
      configurable: true,
      get: () => readOnceThenForge(event[fieldName]),
    });
    return output;
  }

  if (fieldName === "eventType") {
    return new Proxy(output, {
      get(target, property, receiver) {
        if (property === "eventType") {
          return readOnceThenForge(event.eventType);
        }
        return Reflect.get(target, property, receiver);
      },
    });
  }

  if (fieldName === "commandId") {
    output.metadata = new Proxy(
      { ...event.metadata },
      {
        get(target, property, receiver) {
          if (property === "commandId") {
            return readOnceThenForge(event.metadata.commandId);
          }
          return Reflect.get(target, property, receiver);
        },
      }
    );
    return output;
  }

  throw new TypeError(`Unsupported changing identity field: ${fieldName}`);
}

test("EventUpcasterRegistry upcasts event across single schema step", () => {
  const registry = new EventUpcasterRegistry();

  registry.register({
    eventType: EVENT_TYPES.ORDER_CREATED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        currency: "USD",
      },
    }),
  });

  const eventV1 = createDomainEvent({
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 1,
    sequence: 1,
    payload: { item: "Book", quantity: 2 },
  });

  assert.equal(eventV1.metadata.schemaVersion, 1);

  const eventV2 = registry.upcast(eventV1);

  assert.equal(eventV2.metadata.schemaVersion, 2);
  assert.equal(eventV2.payload.currency, "USD");
  assert.equal(eventV2.payload.item, "Book");
  assert.equal(eventV2.payload.quantity, 2);
  assert.equal(eventV2.eventId, eventV1.eventId);
});

test("EventUpcasterRegistry upcasts event across multiple chained schema steps", () => {
  const registry = new EventUpcasterRegistry();

  registry.register({
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        currency: "EUR",
      },
    }),
  });

  registry.register({
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    fromVersion: 2,
    toVersion: 3,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        paymentGateway: "stripe_mock",
      },
    }),
  });

  const eventV1 = createDomainEvent({
    eventType: EVENT_TYPES.PAYMENT_CHARGED,
    aggregateId: 1,
    sequence: 3,
    payload: { paymentId: 101, amount: 250 },
  });

  const eventV3 = registry.upcast(eventV1);

  assert.equal(eventV3.metadata.schemaVersion, 3);
  assert.equal(eventV3.payload.currency, "EUR");
  assert.equal(eventV3.payload.paymentGateway, "stripe_mock");
  assert.equal(eventV3.payload.amount, 250);
});

test("EventUpcasterRegistry leaves events without registered upcasters unchanged", () => {
  const registry = new EventUpcasterRegistry();
  const event = createDomainEvent({
    eventType: EVENT_TYPES.INVENTORY_RESERVED,
    aggregateId: 1,
    sequence: 2,
    payload: { reservationId: 1, item: "Book", quantity: 2 },
  });

  const unchanged = registry.upcast(event);
  assert.equal(unchanged, event);
});

test("InMemoryEventStore seamlessly upcasts events on read when upcasterRegistry is configured", () => {
  const registry = new EventUpcasterRegistry();
  registry.register({
    eventType: EVENT_TYPES.ORDER_CREATED,
    fromVersion: 1,
    toVersion: 2,
    upcast: (event) => ({
      ...event,
      payload: {
        ...event.payload,
        normalizedTag: "v2_migrated",
      },
    }),
  });

  const store = new InMemoryEventStore({ upcasterRegistry: registry });
  const eventV1 = createDomainEvent({
    eventType: EVENT_TYPES.ORDER_CREATED,
    aggregateId: 42,
    sequence: 1,
    payload: { item: "Laptop", quantity: 1 },
  });

  store.append(eventV1, { expectedVersion: 0 });

  const readEvents = store.getByAggregateId(42);
  assert.equal(readEvents.length, 1);
  assert.equal(readEvents[0].metadata.schemaVersion, 2);
  assert.equal(readEvents[0].payload.normalizedTag, "v2_migrated");
});

test("EventUpcasterRegistry rejects invalid registration inputs", () => {
  const registry = new EventUpcasterRegistry();

  assert.throws(
    () => registry.register({ eventType: "", fromVersion: 1, toVersion: 2, upcast: () => {} }),
    TypeError
  );

  assert.throws(
    () => registry.register({ eventType: "ORDER_CREATED", fromVersion: 2, toVersion: 1, upcast: () => {} }),
    /toVersion must be strictly greater/
  );

  assert.throws(
    () => registry.register({ eventType: "ORDER_CREATED", fromVersion: 1, toVersion: 2, upcast: null }),
    TypeError
  );
});

const identityMutations = {
  eventId: (event) => ({ ...event, eventId: "forged-event" }),
  aggregateId: (event) => ({ ...event, aggregateId: 999 }),
  sequence: (event) => ({ ...event, sequence: 9 }),
  timestamp: (event) => ({ ...event, timestamp: "2030-01-01T00:00:00.000Z" }),
  commandId: (event) => ({
    ...event,
    metadata: { ...event.metadata, commandId: "forged-command" },
  }),
  correlationId: (event) => ({
    ...event,
    metadata: { ...event.metadata, correlationId: "forged-correlation" },
  }),
  causationId: (event) => ({
    ...event,
    metadata: { ...event.metadata, causationId: "forged-causation" },
  }),
  eventType: (event) => ({
    ...event,
    eventType: EVENT_TYPES.ORDER_DELETED,
  }),
};

for (const [fieldName, mutate] of Object.entries(identityMutations)) {
  test(`EventUpcasterRegistry rejects ${fieldName} mutation`, () => {
    const registry = new EventUpcasterRegistry();
    const rawEvent = createHistoricalOrderEvent();

    registerOrderUpcaster(registry, (event) => mutate({
      ...event,
      payload: { ...event.payload, currency: "USD" },
    }));

    assert.throws(
      () => registry.upcast(rawEvent),
      (error) =>
        assertUpcastError("EVENT_UPCAST_IDENTITY_VIOLATION")(error) &&
        error.field === fieldName &&
        error.eventId === rawEvent.eventId &&
        error.aggregateId === rawEvent.aggregateId
    );
  });
}

test("EventUpcasterRegistry rejects skipped schema versions at registration", () => {
  const registry = new EventUpcasterRegistry();

  assert.throws(
    () => registerOrderUpcaster(
      registry,
      (event) => ({ ...event }),
      { fromVersion: 1, toVersion: 3 }
    ),
    assertUpcastError("EVENT_UPCAST_OUTPUT_INVALID", "NON_CONTIGUOUS_MIGRATION")
  );
});

test("EventUpcasterRegistry rejects callback-owned schemaVersion", () => {
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    metadata: { ...event.metadata, schemaVersion: 2 },
  }));

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError(
      "EVENT_UPCAST_OUTPUT_INVALID",
      "CALLBACK_SCHEMA_VERSION_MUTATION"
    )
  );
});

test("EventUpcasterRegistry rejects a backwards target version", () => {
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(
    registry,
    (event) => ({ ...event }),
    { fromVersion: 2, toVersion: 3 }
  );
  const eventV2 = {
    ...createHistoricalOrderEvent(),
    metadata: {
      ...createHistoricalOrderEvent().metadata,
      schemaVersion: 2,
    },
  };

  assert.throws(
    () => registry.upcast(eventV2, 1),
    assertUpcastError("EVENT_UPCAST_OUTPUT_INVALID", "BACKWARD_MIGRATION")
  );
});

test("EventUpcasterRegistry rejects an unknown migration path", () => {
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(
    registry,
    (event) => ({ ...event }),
    { fromVersion: 2, toVersion: 3 }
  );

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError("EVENT_UPCAST_OUTPUT_INVALID", "MIGRATION_PATH_MISSING")
  );
});

test("EventUpcasterRegistry never returns a partially migrated event", () => {
  const registry = new EventUpcasterRegistry();
  const rawEvent = createHistoricalOrderEvent();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    payload: { ...event.payload, currency: "USD" },
  }));

  assert.throws(
    () => registry.upcast(rawEvent, 3),
    assertUpcastError("EVENT_UPCAST_OUTPUT_INVALID", "MIGRATION_PATH_MISSING")
  );
  assert.equal(rawEvent.metadata.schemaVersion, 1);
  assert.equal(rawEvent.payload.currency, undefined);
});

test("EventUpcasterRegistry rejects invalid event output", () => {
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, () => []);

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError("EVENT_UPCAST_OUTPUT_INVALID", "INVALID_EVENT")
  );
});

for (const [fieldName, forgedValue] of [
  ["eventId", "forged-event"],
  ["aggregateId", 999],
  ["sequence", 9],
  ["eventType", EVENT_TYPES.ORDER_DELETED],
  ["commandId", "forged-command"],
]) {
  test(`F-3: ${fieldName} accessor cannot change identity after validation`, () => {
    const registry = new EventUpcasterRegistry();
    const rawEvent = createHistoricalOrderEvent();
    const readCounters = [];
    registerOrderUpcaster(registry, (event) =>
      createChangingIdentityOutput(
        event,
        fieldName,
        forgedValue,
        readCounters
      )
    );

    const migrated = registry.upcast(rawEvent);

    assert.equal(migrated.eventId, rawEvent.eventId);
    assert.equal(migrated.aggregateId, rawEvent.aggregateId);
    assert.equal(migrated.sequence, rawEvent.sequence);
    assert.equal(migrated.eventType, rawEvent.eventType);
    assert.equal(migrated.metadata.commandId, rawEvent.metadata.commandId);
    assert.equal(migrated.metadata.schemaVersion, 2);
    assert.deepEqual(
      readCounters.map((counter) => counter.reads),
      [1, 1],
      "each determinism run must materialize the changing property exactly once"
    );
  });
}

test("F-3: materialized upcast output is deeply immutable and stable", () => {
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    payload: {
      ...event.payload,
      currency: "USD",
      pricing: { amountInCents: 1250 },
    },
  }));

  const migrated = registry.upcast(createHistoricalOrderEvent());

  assert.equal(Object.isFrozen(migrated), true);
  assert.equal(Object.isFrozen(migrated.metadata), true);
  assert.equal(Object.isFrozen(migrated.payload), true);
  assert.equal(Object.isFrozen(migrated.payload.pricing), true);
  assert.equal(Reflect.set(migrated, "eventId", "forged-event"), false);
  assert.equal(Reflect.set(migrated.metadata, "commandId", "forged-command"), false);
  assert.equal(Reflect.set(migrated.payload.pricing, "amountInCents", 0), false);
  assert.equal(migrated.eventId, "event-v1");
  assert.equal(migrated.metadata.commandId, "command-v1");
  assert.equal(migrated.payload.pricing.amountInCents, 1250);
});

test("F-3: memory and SQLite expose the same stable identity snapshot", () => {
  const results = {};

  for (const adapterName of ["memory", "sqlite"]) {
    const registry = new EventUpcasterRegistry();
    registerOrderUpcaster(registry, (event) =>
      createChangingIdentityOutput(
        event,
        "eventType",
        EVENT_TYPES.ORDER_DELETED
      )
    );
    const { eventStore, close } = createEventStore(adapterName, registry);

    try {
      const rawEvent = createHistoricalOrderEvent();
      eventStore.append(rawEvent, { expectedVersion: 0 });
      const byAggregate = eventStore.getByAggregateId(rawEvent.aggregateId)[0];
      const byCommand = eventStore.getByCommandId(rawEvent.metadata.commandId)[0];

      results[adapterName] = { byAggregate, byCommand };
      assert.equal(byAggregate.eventType, rawEvent.eventType);
      assert.equal(byCommand.eventType, rawEvent.eventType);
      assert.equal(Object.isFrozen(byAggregate), true);
      assert.equal(Object.isFrozen(byCommand), true);
    } finally {
      close();
    }
  }

  assert.deepEqual(results.sqlite, results.memory);
});

test("EventUpcasterRegistry permits value-preserving payload migrations", () => {
  const registry = new EventUpcasterRegistry();
  const rawEvent = createHistoricalOrderEvent({
    payload: {
      item: "Book",
      quantity: 2,
      legacySku: "BOOK-001",
      amount: 12.5,
    },
  });

  registerOrderUpcaster(registry, (event) => {
    const { legacySku, ...payload } = event.payload;
    return {
      ...event,
      payload: {
        ...payload,
        sku: legacySku,
        currency: "USD",
        amountInCents: payload.amount * 100,
      },
    };
  });

  const migrated = registry.upcast(rawEvent);
  assert.deepEqual(migrated.payload, {
    item: "Book",
    quantity: 2,
    amount: 12.5,
    sku: "BOOK-001",
    currency: "USD",
    amountInCents: 1250,
  });
});

test("EventUpcasterRegistry rejects Date.now-dependent output", (t) => {
  const originalDateNow = Date.now;
  let now = 1000;
  Date.now = () => ++now;
  t.after(() => { Date.now = originalDateNow; });

  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    payload: { ...event.payload, migratedAt: Date.now() },
  }));

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError("EVENT_UPCAST_NON_DETERMINISTIC", "OUTPUT_MISMATCH")
  );
});

test("EventUpcasterRegistry rejects Math.random-dependent output", (t) => {
  const originalRandom = Math.random;
  let random = 0;
  Math.random = () => (random += 0.1);
  t.after(() => { Math.random = originalRandom; });

  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    payload: { ...event.payload, migrationNonce: Math.random() },
  }));

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError("EVENT_UPCAST_NON_DETERMINISTIC", "OUTPUT_MISMATCH")
  );
});

test("EventUpcasterRegistry rejects mutable closure output", () => {
  let invocation = 0;
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    payload: { ...event.payload, invocation: ++invocation },
  }));

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError("EVENT_UPCAST_NON_DETERMINISTIC", "OUTPUT_MISMATCH")
  );
});

test("EventUpcasterRegistry rejects mutable configuration output", () => {
  let reads = 0;
  const configuration = {
    get currency() {
      reads += 1;
      return reads === 1 ? "USD" : "EUR";
    },
  };
  const registry = new EventUpcasterRegistry();
  registerOrderUpcaster(registry, (event) => ({
    ...event,
    payload: { ...event.payload, currency: configuration.currency },
  }));

  assert.throws(
    () => registry.upcast(createHistoricalOrderEvent()),
    assertUpcastError("EVENT_UPCAST_NON_DETERMINISTIC", "OUTPUT_MISMATCH")
  );
});

for (const adapterName of ["memory", "sqlite"]) {
  test(`${adapterName} Event Store rejects invalid upcasts before aggregate and command reads`, (t) => {
    const registry = new EventUpcasterRegistry();
    registerOrderUpcaster(registry, (event) => ({
      ...event,
      eventId: "forged-event",
    }));
    const { eventStore, close } = createEventStore(adapterName, registry);
    t.after(close);
    const rawEvent = createHistoricalOrderEvent();
    eventStore.append(rawEvent, { expectedVersion: 0 });

    for (const read of [
      () => eventStore.getByAggregateId(rawEvent.aggregateId),
      () => eventStore.getByCommandId(rawEvent.metadata.commandId),
    ]) {
      assert.throws(
        read,
        assertUpcastError("EVENT_UPCAST_IDENTITY_VIOLATION")
      );
    }
  });

  test(`${adapterName} replay and snapshot replay stay deterministic after valid upcasting`, (t) => {
    const registry = new EventUpcasterRegistry();
    registerOrderUpcaster(registry, (event) => ({
      ...event,
      payload: { ...event.payload, currency: "USD" },
    }));
    const { eventStore, close } = createEventStore(adapterName, registry);
    t.after(close);
    const rawEvent = createHistoricalOrderEvent();
    eventStore.append(rawEvent, { expectedVersion: 0 });
    const engine = new RollbackEngine({
      eventStore,
      clock: () => FIXED_TIMESTAMP,
    });

    const firstReplay = engine.replay(rawEvent.aggregateId);
    const secondReplay = engine.replay(rawEvent.aggregateId);
    engine.createSnapshot(rawEvent.aggregateId);
    const snapshotReplay = engine.replayFromSnapshot(rawEvent.aggregateId);

    assert.deepEqual(secondReplay, firstReplay);
    assert.deepEqual(snapshotReplay, firstReplay);
    assert.equal(rawEvent.metadata.schemaVersion, 1);
    assert.equal(rawEvent.payload.currency, undefined);
  });
}
