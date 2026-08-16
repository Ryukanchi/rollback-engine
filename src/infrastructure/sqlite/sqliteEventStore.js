const { assertDomainEvent } = require("../../domain/events");
const {
  createFencingTokenStaleError,
  createFencingTokenRequiredError,
  createCommandLeaseExpiredError,
} = require("../../application/errors");

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function assertAggregateId(aggregateId) {
  if (!isIdentifier(aggregateId)) {
    throw new TypeError(
      "aggregateId must be a non-empty string or a positive safe integer"
    );
  }
}

function assertExpectedVersion(expectedVersion) {
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 0) {
    throw new TypeError("expectedVersion must be a non-negative safe integer");
  }
}

function createOptimisticConcurrencyError(
  aggregateId,
  expectedVersion,
  actualVersion
) {
  const error = new Error(
    `Expected aggregate ${aggregateId} at version ${expectedVersion}, but current version is ${actualVersion}`
  );
  error.code = "OPTIMISTIC_CONCURRENCY_CONFLICT";
  error.aggregateId = aggregateId;
  error.expectedVersion = expectedVersion;
  error.actualVersion = actualVersion;
  return error;
}

function deepFreeze(value, seen = new WeakSet()) {
  if (!value || typeof value !== "object" || seen.has(value)) {
    return value;
  }

  seen.add(value);

  for (const nestedValue of Object.values(value)) {
    deepFreeze(nestedValue, seen);
  }

  return Object.freeze(value);
}

function rowToEvent(row) {
  if (!row) {
    return null;
  }

  return deepFreeze({
    eventId: row.event_id,
    eventType: row.event_type,
    aggregateId: row.aggregate_id,
    sequence: Number(row.sequence),
    timestamp: row.timestamp,
    payload: JSON.parse(row.payload),
    metadata: JSON.parse(row.metadata),
  });
}

class SqliteEventStore {
  #db;

  #subscribers = new Set();

  #upcasterRegistry;

  #now;

  #stmtInsertEvent;

  #stmtLastEventByAggregate;

  #stmtEventsByAggregate;

  #stmtEventsByAggregateAfter;

  #stmtEventsByCommandId;

  #stmtAllEvents;

  #stmtLastSequence;

  #stmtGetCommandLease;

  constructor({ db, upcasterRegistry, now = () => Date.now() } = {}) {
    if (!db || typeof db.prepare !== "function") {
      throw new TypeError("db must be a valid SQLite database instance");
    }

    this.#db = db;
    this.#upcasterRegistry = upcasterRegistry;
    this.#now = typeof now === "function" ? now : () => Date.now();

    this.#stmtInsertEvent = this.#db.prepare(`
      INSERT INTO events (
        event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.#stmtLastEventByAggregate = this.#db.prepare(`
      SELECT sequence, timestamp FROM events
      WHERE aggregate_id = ?
      ORDER BY sequence DESC
      LIMIT 1
    `);

    this.#stmtEventsByAggregate = this.#db.prepare(`
      SELECT event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata
      FROM events
      WHERE aggregate_id = ?
      ORDER BY sequence ASC
    `);

    this.#stmtEventsByAggregateAfter = this.#db.prepare(`
      SELECT event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata
      FROM events
      WHERE aggregate_id = ? AND sequence > ?
      ORDER BY sequence ASC
    `);

    this.#stmtEventsByCommandId = this.#db.prepare(`
      SELECT event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata
      FROM events
      WHERE command_id = ?
      ORDER BY rowid ASC
    `);

    this.#stmtAllEvents = this.#db.prepare(`
      SELECT event_id, aggregate_id, sequence, command_id, event_type, timestamp, payload, metadata
      FROM events
      ORDER BY rowid ASC
    `);

    this.#stmtLastSequence = this.#db.prepare(`
      SELECT max(sequence) as last_seq FROM events WHERE aggregate_id = ?
    `);

    this.#stmtGetCommandLease = this.#db.prepare(`
      SELECT status, lease_owner, lease_token, lease_expires_at
      FROM commands
      WHERE command_id = ?
    `);
  }

  setNow(now) {
    this.#now = typeof now === "function" ? now : () => Date.now();
  }

  append(event, { expectedVersion, fencingToken } = {}) {
    assertDomainEvent(event);
    assertExpectedVersion(expectedVersion);

    this.#db.exec("BEGIN IMMEDIATE");

    try {
      // Fencing check inside authoritative append transaction
      if (event.metadata?.commandId) {
        const cmdRow = this.#stmtGetCommandLease.get(event.metadata.commandId);
        if (cmdRow) {
          if (cmdRow.status !== "processing") {
            throw createFencingTokenStaleError({
              commandId: event.metadata.commandId,
              providedToken: fencingToken,
              currentToken: cmdRow.lease_token !== null ? Number(cmdRow.lease_token) : null,
              leaseOwner: cmdRow.lease_owner,
            });
          }

          if (cmdRow.lease_token !== null && cmdRow.lease_token !== undefined) {
            const currentToken = Number(cmdRow.lease_token);
            if (fencingToken === undefined || fencingToken === null) {
              throw createFencingTokenRequiredError({
                commandId: event.metadata.commandId,
                leaseOwner: cmdRow.lease_owner,
              });
            }

            if (Number(fencingToken) !== currentToken) {
              throw createFencingTokenStaleError({
                commandId: event.metadata.commandId,
                providedToken: Number(fencingToken),
                currentToken,
                leaseOwner: cmdRow.lease_owner,
              });
            }

            if (cmdRow.lease_expires_at !== null && cmdRow.lease_expires_at !== undefined) {
              const nowMs = this.#now();
              if (Number(cmdRow.lease_expires_at) < nowMs) {
                throw createCommandLeaseExpiredError({
                  commandId: event.metadata.commandId,
                  fencingToken: Number(fencingToken),
                  leaseExpiresAt: Number(cmdRow.lease_expires_at),
                  now: nowMs,
                });
              }
            }
          }
        }
      }

      const lastRow = this.#stmtLastEventByAggregate.get(event.aggregateId);
      const actualVersion = lastRow ? Number(lastRow.sequence) : 0;

      if (actualVersion !== expectedVersion) {
        throw createOptimisticConcurrencyError(
          event.aggregateId,
          expectedVersion,
          actualVersion
        );
      }

      const expectedSequence = expectedVersion + 1;

      if (event.sequence !== expectedSequence) {
        throw new Error(
          `Expected event sequence ${expectedSequence} for aggregate ${event.aggregateId}, received ${event.sequence}`
        );
      }

      if (
        lastRow &&
        new Date(event.timestamp).getTime() < new Date(lastRow.timestamp).getTime()
      ) {
        throw new Error(
          `Event timestamp cannot move backwards for aggregate ${event.aggregateId}`
        );
      }

      try {
        this.#stmtInsertEvent.run(
          event.eventId,
          event.aggregateId,
          event.sequence,
          event.metadata.commandId,
          event.eventType,
          event.timestamp,
          JSON.stringify(event.payload),
          JSON.stringify(event.metadata)
        );
      } catch (insertError) {
        if (
          insertError?.message?.includes("UNIQUE constraint failed") &&
          insertError.message.includes("events.event_id")
        ) {
          throw new Error(`Event ID ${event.eventId} already exists`);
        }

        if (
          insertError?.message?.includes("UNIQUE constraint failed") &&
          insertError.message.includes("events.aggregate_id")
        ) {
          const recheckedRow = this.#stmtLastEventByAggregate.get(event.aggregateId);
          const currentVer = recheckedRow ? Number(recheckedRow.sequence) : actualVersion;
          throw createOptimisticConcurrencyError(
            event.aggregateId,
            expectedVersion,
            currentVer
          );
        }

        throw insertError;
      }

      this.#db.exec("COMMIT");
    } catch (error) {
      try {
        this.#db.exec("ROLLBACK");
      } catch {
        // Rollback if active
      }
      throw error;
    }

    const storedEvent = deepFreeze(structuredClone(event));
    this.#notifySubscribers(storedEvent);
    return storedEvent;
  }

  #transform(event) {
    if (!this.#upcasterRegistry || !event) {
      return event;
    }
    return this.#upcasterRegistry.upcast(event);
  }

  getByAggregateId(aggregateId) {
    assertAggregateId(aggregateId);
    const rows = this.#stmtEventsByAggregate.all(aggregateId);
    return rows.map((row) => this.#transform(rowToEvent(row)));
  }

  getByAggregateIdAfter(aggregateId, sequence) {
    assertAggregateId(aggregateId);

    if (!Number.isSafeInteger(sequence) || sequence < 0) {
      throw new TypeError("sequence must be a non-negative safe integer");
    }

    const rows = this.#stmtEventsByAggregateAfter.all(aggregateId, sequence);
    return rows.map((row) => this.#transform(rowToEvent(row)));
  }

  getByCommandId(commandId) {
    if (!isIdentifier(commandId)) {
      throw new TypeError(
        "commandId must be a non-empty string or a positive safe integer"
      );
    }

    const rows = this.#stmtEventsByCommandId.all(commandId);
    return rows.map((row) => this.#transform(rowToEvent(row)));
  }

  getAll() {
    const rows = this.#stmtAllEvents.all();
    return rows.map((row) => this.#transform(rowToEvent(row)));
  }

  getLastSequence(aggregateId) {
    assertAggregateId(aggregateId);
    const row = this.#stmtLastSequence.get(aggregateId);
    return row && row.last_seq !== null ? Number(row.last_seq) : 0;
  }

  subscribe(filterOrHandler, maybeHandler) {
    let filter = {};
    let handler = filterOrHandler;

    if (typeof filterOrHandler === "object" && filterOrHandler !== null) {
      filter = filterOrHandler;
      handler = maybeHandler;
    }

    if (typeof handler !== "function") {
      throw new TypeError("subscription handler must be a function");
    }

    const subscription = { filter, handler };
    this.#subscribers.add(subscription);

    return () => {
      this.#subscribers.delete(subscription);
    };
  }

  #notifySubscribers(event) {
    for (const subscription of this.#subscribers) {
      const { filter, handler } = subscription;

      if (
        filter.aggregateId !== undefined &&
        filter.aggregateId !== event.aggregateId
      ) {
        continue;
      }

      if (
        filter.eventType !== undefined &&
        filter.eventType !== event.eventType
      ) {
        continue;
      }

      try {
        const result = handler(event);
        if (result && typeof result.catch === "function") {
          result.catch(() => {});
        }
      } catch {
        // Subscribers are asynchronous / side-effects with error isolation.
      }
    }
  }
}

module.exports = {
  SqliteEventStore,
};
