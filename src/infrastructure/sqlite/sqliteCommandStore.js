const { isDeepStrictEqual } = require("node:util");
const { COMMAND_STATUSES } = require("../../application/storeContracts");

function assertNonEmptyString(value, fieldName) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
}

function assertPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new TypeError("payload must be an object");
  }
}

function clone(value) {
  return value === null || value === undefined ? value : structuredClone(value);
}

function buildEventRange(events, commandId) {
  if (!Array.isArray(events) || events.length === 0) {
    throw new TypeError("events must be a non-empty array");
  }

  const normalizedEvents = clone(events);
  const [firstEvent] = normalizedEvents;

  for (const [index, event] of normalizedEvents.entries()) {
    if (
      !event ||
      typeof event !== "object" ||
      typeof event.eventId !== "string" ||
      !Number.isSafeInteger(event.sequence) ||
      event.sequence <= 0
    ) {
      throw new TypeError("event must contain an eventId and positive sequence");
    }

    if (event.metadata?.commandId !== commandId) {
      throw new Error(`Event ${event.eventId} does not belong to command ${commandId}`);
    }

    if (event.aggregateId !== firstEvent.aggregateId) {
      throw new Error("Command events cannot span multiple aggregates");
    }

    if (event.sequence !== firstEvent.sequence + index) {
      throw new Error("Command events must be contiguous");
    }
  }

  const lastEvent = normalizedEvents[normalizedEvents.length - 1];

  return {
    aggregateId: firstEvent.aggregateId,
    firstSequence: firstEvent.sequence,
    lastSequence: lastEvent.sequence,
    eventIds: normalizedEvents.map((event) => event.eventId),
  };
}

function rowToRecord(row) {
  if (!row) {
    return null;
  }

  return {
    commandId: row.command_id,
    commandType: row.command_type,
    payload: JSON.parse(row.payload),
    status: row.status,
    aggregateId: row.aggregate_id ?? null,
    eventRange: row.event_range ? JSON.parse(row.event_range) : null,
    result: row.result ? JSON.parse(row.result) : null,
    error: row.error ? JSON.parse(row.error) : null,
  };
}

class SqliteCommandStore {
  #db;

  #stmtGetCommand;

  #stmtInsertCommand;

  #stmtUpdateEventRange;

  #stmtCompleteCommand;

  #stmtFailCommand;

  #stmtDeleteCommand;

  #stmtReconcileFailure;

  constructor({ db } = {}) {
    if (!db || typeof db.prepare !== "function") {
      throw new TypeError("db must be a valid SQLite database instance");
    }

    this.#db = db;

    this.#stmtGetCommand = this.#db.prepare(`
      SELECT command_id, command_type, payload, status, aggregate_id, event_range, result, error
      FROM commands
      WHERE command_id = ?
    `);

    this.#stmtInsertCommand = this.#db.prepare(`
      INSERT INTO commands (
        command_id, command_type, payload, status, aggregate_id, event_range, result, error
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.#stmtUpdateEventRange = this.#db.prepare(`
      UPDATE commands
      SET aggregate_id = ?, event_range = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtCompleteCommand = this.#db.prepare(`
      UPDATE commands
      SET status = ?, result = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtFailCommand = this.#db.prepare(`
      UPDATE commands
      SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtDeleteCommand = this.#db.prepare(`
      DELETE FROM commands WHERE command_id = ?
    `);

    this.#stmtReconcileFailure = this.#db.prepare(`
      UPDATE commands
      SET aggregate_id = ?, event_range = ?, error = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);
  }

  reserve({ commandId, commandType, payload } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(commandType, "commandType");
    assertPayload(payload);

    const existingRow = this.#stmtGetCommand.get(commandId);

    if (existingRow) {
      const existing = rowToRecord(existingRow);
      const conflict =
        existing.commandType !== commandType ||
        !isDeepStrictEqual(existing.payload, payload);

      return {
        created: false,
        conflict,
        record: clone(existing),
      };
    }

    const record = {
      commandId,
      commandType,
      payload: clone(payload),
      status: COMMAND_STATUSES.PROCESSING,
      aggregateId: null,
      eventRange: null,
      result: null,
      error: null,
    };

    this.#stmtInsertCommand.run(
      commandId,
      commandType,
      JSON.stringify(payload),
      COMMAND_STATUSES.PROCESSING,
      null,
      null,
      null,
      null
    );

    return {
      created: true,
      conflict: false,
      record: clone(record),
    };
  }

  recordEvent(commandId, event) {
    const record = this.#requireProcessing(commandId);

    if (
      !event ||
      typeof event !== "object" ||
      typeof event.eventId !== "string" ||
      !Number.isSafeInteger(event.sequence) ||
      event.sequence <= 0
    ) {
      throw new TypeError("event must contain an eventId and positive sequence");
    }

    if (event.metadata?.commandId !== commandId) {
      throw new Error(`Event ${event.eventId} does not belong to command ${commandId}`);
    }

    if (!record.eventRange) {
      record.aggregateId = event.aggregateId;
      record.eventRange = {
        aggregateId: event.aggregateId,
        firstSequence: event.sequence,
        lastSequence: event.sequence,
        eventIds: [event.eventId],
      };

      this.#stmtUpdateEventRange.run(
        record.aggregateId,
        JSON.stringify(record.eventRange),
        commandId
      );

      return clone(record);
    }

    if (record.eventRange.aggregateId !== event.aggregateId) {
      throw new Error(`Command ${commandId} cannot span multiple aggregates`);
    }

    if (event.sequence !== record.eventRange.lastSequence + 1) {
      throw new Error(`Command ${commandId} events must be contiguous`);
    }

    record.eventRange.lastSequence = event.sequence;
    record.eventRange.eventIds.push(event.eventId);

    this.#stmtUpdateEventRange.run(
      record.aggregateId,
      JSON.stringify(record.eventRange),
      commandId
    );

    return clone(record);
  }

  complete(commandId, result) {
    const record = this.#requireProcessing(commandId);
    const clonedResult = clone(result);

    record.status = COMMAND_STATUSES.COMPLETED;
    record.result = clonedResult;

    this.#stmtCompleteCommand.run(
      COMMAND_STATUSES.COMPLETED,
      JSON.stringify(clonedResult),
      commandId
    );

    return clone(record);
  }

  fail(commandId, error) {
    const record = this.#requireProcessing(commandId);

    if (!error || typeof error !== "object" || Array.isArray(error)) {
      throw new TypeError("error must be an object");
    }

    const clonedError = clone(error);

    record.status = COMMAND_STATUSES.FAILED;
    record.error = clonedError;

    this.#stmtFailCommand.run(
      COMMAND_STATUSES.FAILED,
      JSON.stringify(clonedError),
      commandId
    );

    return clone(record);
  }

  release(commandId) {
    assertNonEmptyString(commandId, "commandId");

    const row = this.#stmtGetCommand.get(commandId);

    if (!row) {
      return false;
    }

    const record = rowToRecord(row);

    if (record.status !== COMMAND_STATUSES.PROCESSING || record.eventRange) {
      throw new Error(`Command ${commandId} cannot be released`);
    }

    this.#stmtDeleteCommand.run(commandId);
    return true;
  }

  releaseFailed(commandId, expectedErrorCode) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(expectedErrorCode, "expectedErrorCode");

    const row = this.#stmtGetCommand.get(commandId);

    if (!row) {
      return false;
    }

    const record = rowToRecord(row);

    if (
      record.status !== COMMAND_STATUSES.FAILED ||
      record.eventRange ||
      record.error?.code !== expectedErrorCode
    ) {
      throw new Error(`Failed command ${commandId} cannot be released`);
    }

    this.#stmtDeleteCommand.run(commandId);
    return true;
  }

  get(commandId) {
    assertNonEmptyString(commandId, "commandId");
    const row = this.#stmtGetCommand.get(commandId);
    return row ? clone(rowToRecord(row)) : null;
  }

  reconcileEvents(commandId, events) {
    const record = this.#requireProcessing(commandId);
    const eventRange = buildEventRange(events, commandId);

    record.aggregateId = eventRange.aggregateId;
    record.eventRange = eventRange;

    this.#stmtUpdateEventRange.run(
      record.aggregateId,
      JSON.stringify(record.eventRange),
      commandId
    );

    return clone(record);
  }

  reconcileFailure(commandId, events, error) {
    assertNonEmptyString(commandId, "commandId");

    const row = this.#stmtGetCommand.get(commandId);
    const record = rowToRecord(row);

    if (!record || record.status !== COMMAND_STATUSES.FAILED) {
      throw new Error(`Command ${commandId} is not failed`);
    }

    if (!error || typeof error !== "object" || Array.isArray(error)) {
      throw new TypeError("error must be an object");
    }

    const eventRange = buildEventRange(events, commandId);
    const clonedError = clone(error);

    record.aggregateId = eventRange.aggregateId;
    record.eventRange = eventRange;
    record.error = clonedError;

    this.#stmtReconcileFailure.run(
      record.aggregateId,
      JSON.stringify(record.eventRange),
      JSON.stringify(clonedError),
      commandId
    );

    return clone(record);
  }

  #requireProcessing(commandId) {
    assertNonEmptyString(commandId, "commandId");

    const row = this.#stmtGetCommand.get(commandId);

    if (!row) {
      throw new Error(`Command ${commandId} is not reserved`);
    }

    const record = rowToRecord(row);

    if (record.status !== COMMAND_STATUSES.PROCESSING) {
      throw new Error(`Command ${commandId} is not processing`);
    }

    return record;
  }
}

module.exports = {
  SqliteCommandStore,
};
