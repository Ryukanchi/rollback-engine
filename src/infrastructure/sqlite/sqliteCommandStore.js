const { isDeepStrictEqual } = require("node:util");
const { COMMAND_STATUSES } = require("../../application/storeContracts");
const {
  createFencingTokenStaleError,
  createCommandLeaseExpiredError,
} = require("../../application/errors");

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
    leaseOwner: row.lease_owner ?? null,
    leaseToken: row.lease_token !== null && row.lease_token !== undefined ? Number(row.lease_token) : 1,
    leaseExpiresAt: row.lease_expires_at !== null && row.lease_expires_at !== undefined ? Number(row.lease_expires_at) : null,
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

  #stmtTakeOver;

  #stmtRenewLease;

  #stmtCountEvents;

  constructor({ db } = {}) {
    if (!db || typeof db.prepare !== "function") {
      throw new TypeError("db must be a valid SQLite database instance");
    }

    this.#db = db;

    this.#stmtGetCommand = this.#db.prepare(`
      SELECT command_id, command_type, payload, status, aggregate_id, event_range, result, error, lease_owner, lease_token, lease_expires_at
      FROM commands
      WHERE command_id = ?
    `);

    this.#stmtInsertCommand = this.#db.prepare(`
      INSERT INTO commands (
        command_id, command_type, payload, status, aggregate_id, event_range, result, error, lease_owner, lease_token, lease_expires_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.#stmtUpdateEventRange = this.#db.prepare(`
      UPDATE commands
      SET aggregate_id = ?, event_range = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtCompleteCommand = this.#db.prepare(`
      UPDATE commands
      SET status = ?, result = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtFailCommand = this.#db.prepare(`
      UPDATE commands
      SET status = ?, error = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = CURRENT_TIMESTAMP
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

    this.#stmtTakeOver = this.#db.prepare(`
      UPDATE commands
      SET lease_owner = ?, lease_token = ?, lease_expires_at = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtRenewLease = this.#db.prepare(`
      UPDATE commands
      SET lease_expires_at = ?, updated_at = CURRENT_TIMESTAMP
      WHERE command_id = ?
    `);

    this.#stmtCountEvents = this.#db.prepare(`
      SELECT COUNT(*) as cnt FROM events WHERE command_id = ?
    `);
  }

  reserve({
    commandId,
    commandType,
    payload,
    workerId = null,
    leaseTtlMs = 5000,
    now = Date.now(),
  } = {}) {
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

    const leaseOwner = workerId;
    const leaseToken = 1;
    const leaseExpiresAt = workerId ? now + leaseTtlMs : null;

    const record = {
      commandId,
      commandType,
      payload: clone(payload),
      status: COMMAND_STATUSES.PROCESSING,
      aggregateId: null,
      eventRange: null,
      result: null,
      error: null,
      leaseOwner,
      leaseToken,
      leaseExpiresAt,
    };

    this.#stmtInsertCommand.run(
      commandId,
      commandType,
      JSON.stringify(payload),
      COMMAND_STATUSES.PROCESSING,
      null,
      null,
      null,
      null,
      leaseOwner,
      leaseToken,
      leaseExpiresAt
    );

    return {
      created: true,
      conflict: false,
      record: clone(record),
    };
  }

  takeOverExpired({
    commandId,
    workerId,
    leaseTtlMs = 5000,
    now = Date.now(),
    expectedToken,
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(workerId, "workerId");

    this.#db.exec("BEGIN IMMEDIATE;");

    try {
      const row = this.#stmtGetCommand.get(commandId);

      if (!row) {
        this.#db.exec("ROLLBACK;");
        return { success: false, reason: "NOT_FOUND" };
      }

      if (row.status !== COMMAND_STATUSES.PROCESSING) {
        this.#db.exec("ROLLBACK;");
        return { success: false, reason: "NOT_PROCESSING" };
      }

      if (row.event_range) {
        this.#db.exec("ROLLBACK;");
        return { success: false, reason: "HAS_EVENTS" };
      }

      const eventCountRow = this.#stmtCountEvents.get(commandId);
      if (eventCountRow && Number(eventCountRow.cnt) > 0) {
        this.#db.exec("ROLLBACK;");
        return { success: false, reason: "HAS_EVENTS" };
      }

      const expiresAt = row.lease_expires_at !== null ? Number(row.lease_expires_at) : null;
      if (expiresAt !== null && expiresAt > now) {
        this.#db.exec("ROLLBACK;");
        return { success: false, reason: "NOT_EXPIRED" };
      }

      const currentToken = row.lease_token !== null ? Number(row.lease_token) : 1;
      if (expectedToken !== undefined && currentToken !== expectedToken) {
        this.#db.exec("ROLLBACK;");
        return { success: false, reason: "TOKEN_MISMATCH" };
      }

      const newToken = currentToken + 1;
      const newExpiresAt = now + leaseTtlMs;

      this.#stmtTakeOver.run(workerId, newToken, newExpiresAt, commandId);
      this.#db.exec("COMMIT;");

      return { success: true, record: this.get(commandId) };
    } catch (err) {
      try {
        this.#db.exec("ROLLBACK;");
      } catch {}
      throw err;
    }
  }

  renewLease({
    commandId,
    workerId,
    fencingToken,
    leaseTtlMs = 5000,
    now = Date.now(),
  } = {}) {
    assertNonEmptyString(commandId, "commandId");
    assertNonEmptyString(workerId, "workerId");

    this.#db.exec("BEGIN IMMEDIATE;");

    try {
      const row = this.#stmtGetCommand.get(commandId);

      if (!row || row.status !== COMMAND_STATUSES.PROCESSING) {
        throw new Error(`Command ${commandId} is not processing`);
      }

      const currentToken = row.lease_token !== null ? Number(row.lease_token) : 1;

      if (row.lease_owner !== workerId || (fencingToken !== undefined && currentToken !== fencingToken)) {
        throw createFencingTokenStaleError({
          commandId,
          providedToken: fencingToken,
          currentToken,
          workerId,
          leaseOwner: row.lease_owner,
        });
      }

      const expiresAt = row.lease_expires_at !== null ? Number(row.lease_expires_at) : null;
      if (expiresAt !== null && expiresAt <= now) {
        throw createCommandLeaseExpiredError({
          commandId,
          fencingToken: currentToken,
          leaseExpiresAt: expiresAt,
          now,
          workerId,
        });
      }

      const newExpiresAt = now + leaseTtlMs;
      this.#stmtRenewLease.run(newExpiresAt, commandId);
      this.#db.exec("COMMIT;");

      return { renewed: true, leaseExpiresAt: newExpiresAt };
    } catch (err) {
      try {
        this.#db.exec("ROLLBACK;");
      } catch {}
      throw err;
    }
  }

  recordEvent(commandId, event, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

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

  complete(commandId, result, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    const clonedResult = clone(result);

    record.status = COMMAND_STATUSES.COMPLETED;
    record.result = clonedResult;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

    this.#stmtCompleteCommand.run(
      COMMAND_STATUSES.COMPLETED,
      JSON.stringify(clonedResult),
      commandId
    );

    return clone(record);
  }

  fail(commandId, error, { fencingToken } = {}) {
    const record = this.#requireProcessing(commandId);

    if (fencingToken !== undefined && record.leaseToken !== fencingToken) {
      throw createFencingTokenStaleError({
        commandId,
        providedToken: fencingToken,
        currentToken: record.leaseToken,
        leaseOwner: record.leaseOwner,
      });
    }

    if (!error || typeof error !== "object" || Array.isArray(error)) {
      throw new TypeError("error must be an object");
    }

    const clonedError = clone(error);

    record.status = COMMAND_STATUSES.FAILED;
    record.error = clonedError;
    record.leaseOwner = null;
    record.leaseExpiresAt = null;

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
