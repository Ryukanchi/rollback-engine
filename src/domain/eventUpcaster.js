const { isDeepStrictEqual } = require("node:util");

const { assertDomainFact } = require("./events");

const IDENTITY_FIELDS = Object.freeze([
  ["eventId", (event) => event.eventId],
  ["aggregateId", (event) => event.aggregateId],
  ["sequence", (event) => event.sequence],
  ["timestamp", (event) => event.timestamp],
  ["commandId", (event) => event.metadata?.commandId],
  ["correlationId", (event) => event.metadata?.correlationId],
  ["causationId", (event) => event.metadata?.causationId],
  ["eventType", (event) => event.eventType],
]);

function isIdentifier(value) {
  return (
    (typeof value === "string" && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value > 0)
  );
}

function assertPositiveInteger(value, name) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
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

function cloneAndFreeze(value) {
  return deepFreeze(structuredClone(value));
}

function materializePlainCopy(value, seen = new Map()) {
  const valueType = typeof value;

  if (value === null || valueType !== "object") {
    if (valueType === "function" || valueType === "symbol") {
      throw new TypeError("Upcaster output must contain cloneable data values");
    }
    return value;
  }

  if (seen.has(value)) {
    return seen.get(value);
  }

  const isArray = Array.isArray(value);
  const prototype = Reflect.getPrototypeOf(value);

  if (
    (!isArray && prototype !== Object.prototype && prototype !== null) ||
    (isArray && prototype !== Array.prototype)
  ) {
    throw new TypeError("Upcaster output must contain only plain objects and arrays");
  }

  const materialized = isArray ? [] : {};
  seen.set(value, materialized);

  // Enumerate once and read every accepted property exactly once. Accessors
  // and Proxies are allowed at the callback boundary, but only the values
  // captured here can proceed to identity/schema validation.
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key === "symbol") {
      throw new TypeError("Upcaster output cannot contain symbol properties");
    }

    if (isArray && key === "length") {
      continue;
    }

    const descriptor = Reflect.getOwnPropertyDescriptor(value, key);

    if (!descriptor || !descriptor.enumerable) {
      continue;
    }

    const propertyValue = Reflect.get(value, key);
    Object.defineProperty(materialized, key, {
      value: materializePlainCopy(propertyValue, seen),
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }

  return materialized;
}

function materializeAndFreezePlain(value) {
  return deepFreeze(materializePlainCopy(value));
}

function createUpcastError(code, message, {
  sourceEvent,
  eventType,
  fromVersion,
  toVersion,
  targetSchemaVersion,
  field,
  reason,
  cause,
} = {}) {
  const error = new Error(message, cause ? { cause } : undefined);
  error.code = code;

  const rawEventType = sourceEvent?.eventType ?? eventType;
  if (rawEventType !== undefined) error.eventType = rawEventType;
  if (sourceEvent?.eventId !== undefined) error.eventId = sourceEvent.eventId;
  if (sourceEvent?.aggregateId !== undefined) {
    error.aggregateId = sourceEvent.aggregateId;
  }
  if (fromVersion !== undefined) error.fromVersion = fromVersion;
  if (toVersion !== undefined) error.toVersion = toVersion;
  if (targetSchemaVersion !== undefined) {
    error.targetSchemaVersion = targetSchemaVersion;
  }
  if (field !== undefined) error.field = field;
  if (reason !== undefined) error.reason = reason;

  return error;
}

function createOutputInvalidError(message, context = {}) {
  return createUpcastError("EVENT_UPCAST_OUTPUT_INVALID", message, context);
}

function assertIdentityPreserved(sourceEvent, transformed, context) {
  for (const [field, read] of IDENTITY_FIELDS) {
    if (!isDeepStrictEqual(read(sourceEvent), read(transformed))) {
      throw createUpcastError(
        "EVENT_UPCAST_IDENTITY_VIOLATION",
        `Upcaster changed immutable event identity field ${field}`,
        { ...context, sourceEvent, field }
      );
    }
  }
}

function assertStructurallyValidEvent(event, context) {
  try {
    assertDomainFact(event);

    if (!isIdentifier(event.eventId)) {
      throw new TypeError("eventId must be a non-empty string or positive safe integer");
    }

    if (
      !event.metadata ||
      typeof event.metadata !== "object" ||
      Array.isArray(event.metadata)
    ) {
      throw new TypeError("metadata must be an object");
    }

    for (const field of ["commandId", "correlationId", "causationId"]) {
      if (!isIdentifier(event.metadata[field])) {
        throw new TypeError(
          `metadata.${field} must be a non-empty string or positive safe integer`
        );
      }
    }

    if (event.metadata.causationId === event.eventId) {
      throw new TypeError("metadata.causationId cannot reference the event itself");
    }
  } catch (cause) {
    throw createOutputInvalidError(
      `Upcaster for ${context.eventType} v${context.fromVersion} returned an invalid event`,
      { ...context, reason: "INVALID_EVENT", cause }
    );
  }
}

function runValidatedUpcast(upcaster, currentEvent, sourceEvent) {
  const context = {
    sourceEvent,
    eventType: currentEvent.eventType,
    fromVersion: upcaster.fromVersion,
    toVersion: upcaster.toVersion,
  };
  let callbackInput;
  let transformed;

  try {
    callbackInput = cloneAndFreeze(currentEvent);
    transformed = upcaster.upcast(callbackInput);
  } catch (cause) {
    throw createOutputInvalidError(
      `Upcaster for ${currentEvent.eventType} v${upcaster.fromVersion} failed`,
      { ...context, reason: "CALLBACK_FAILED", cause }
    );
  }

  if (!transformed || typeof transformed !== "object" || Array.isArray(transformed)) {
    throw createOutputInvalidError(
      `Upcaster for ${currentEvent.eventType} v${upcaster.fromVersion} returned invalid event`,
      { ...context, reason: "INVALID_EVENT" }
    );
  }

  let materializedOutput;

  try {
    materializedOutput = materializeAndFreezePlain(transformed);
  } catch (cause) {
    throw createOutputInvalidError(
      `Upcaster for ${currentEvent.eventType} v${upcaster.fromVersion} returned unmaterializable output`,
      { ...context, reason: "INVALID_EVENT", cause }
    );
  }

  if (
    !materializedOutput.metadata ||
    materializedOutput.metadata.schemaVersion !== currentEvent.metadata?.schemaVersion
  ) {
    throw createOutputInvalidError(
      `Upcaster for ${currentEvent.eventType} v${upcaster.fromVersion} attempted to control schemaVersion`,
      { ...context, reason: "CALLBACK_SCHEMA_VERSION_MUTATION" }
    );
  }

  let migratedEvent;

  try {
    migratedEvent = deepFreeze({
      ...materializedOutput,
      metadata: {
        ...materializedOutput.metadata,
        schemaVersion: upcaster.toVersion,
      },
    });
  } catch (cause) {
    throw createOutputInvalidError(
      `Upcaster for ${currentEvent.eventType} v${upcaster.fromVersion} returned uncloneable output`,
      { ...context, reason: "INVALID_EVENT", cause }
    );
  }

  // The Registry owns schemaVersion. Validate immutable identity only after
  // that final representation exists, so no callback-controlled object is
  // consulted again after the check.
  assertIdentityPreserved(currentEvent, migratedEvent, context);
  assertStructurallyValidEvent(migratedEvent, context);
  return migratedEvent;
}

class EventUpcasterRegistry {
  #upcasters = new Map();

  #latestSchemaVersions = new Map();

  register({
    eventType,
    fromVersion,
    toVersion,
    upcast,
  } = {}) {
    if (typeof eventType !== "string" || eventType.trim().length === 0) {
      throw new TypeError("eventType must be a non-empty string");
    }

    assertPositiveInteger(fromVersion, "fromVersion");
    assertPositiveInteger(toVersion, "toVersion");

    if (toVersion <= fromVersion) {
      throw createOutputInvalidError(
        "toVersion must be strictly greater than fromVersion",
        {
          eventType,
          fromVersion,
          toVersion,
          reason: "BACKWARD_MIGRATION",
        }
      );
    }

    if (toVersion !== fromVersion + 1) {
      throw createOutputInvalidError(
        "Upcaster schema versions must be contiguous",
        {
          eventType,
          fromVersion,
          toVersion,
          reason: "NON_CONTIGUOUS_MIGRATION",
        }
      );
    }

    if (typeof upcast !== "function") {
      throw new TypeError("upcast must be a function");
    }

    const key = `${eventType}:${fromVersion}`;

    if (this.#upcasters.has(key)) {
      throw new Error(
        `Upcaster for ${eventType} from version ${fromVersion} is already registered`
      );
    }

    this.#upcasters.set(key, {
      eventType,
      fromVersion,
      toVersion,
      upcast,
    });
    this.#latestSchemaVersions.set(
      eventType,
      Math.max(this.#latestSchemaVersions.get(eventType) ?? 1, toVersion)
    );

    return this;
  }

  upcast(event, targetSchemaVersion) {
    if (!event || typeof event !== "object" || Array.isArray(event)) {
      throw new TypeError("event must be an object");
    }

    const currentVersion = event.metadata?.schemaVersion ?? 1;
    assertPositiveInteger(currentVersion, "event.metadata.schemaVersion");

    if (targetSchemaVersion !== undefined) {
      assertPositiveInteger(targetSchemaVersion, "targetSchemaVersion");
    }

    const latestRegisteredVersion = this.#latestSchemaVersions.get(event.eventType);
    const resolvedTargetVersion =
      targetSchemaVersion ?? latestRegisteredVersion ?? currentVersion;

    if (resolvedTargetVersion < currentVersion) {
      throw createOutputInvalidError(
        `Cannot migrate ${event.eventType} backwards from v${currentVersion} to v${resolvedTargetVersion}`,
        {
          sourceEvent: event,
          fromVersion: currentVersion,
          targetSchemaVersion: resolvedTargetVersion,
          reason: "BACKWARD_MIGRATION",
        }
      );
    }

    let currentEvent = event;
    let version = currentVersion;

    while (version < resolvedTargetVersion) {
      const key = `${currentEvent.eventType}:${version}`;
      const upcaster = this.#upcasters.get(key);

      if (!upcaster) {
        throw createOutputInvalidError(
          `Missing upcaster for ${currentEvent.eventType} from v${version} to target v${resolvedTargetVersion}`,
          {
            sourceEvent: event,
            fromVersion: version,
            targetSchemaVersion: resolvedTargetVersion,
            reason: "MIGRATION_PATH_MISSING",
          }
        );
      }

      const firstResult = runValidatedUpcast(upcaster, currentEvent, event);
      const secondResult = runValidatedUpcast(upcaster, currentEvent, event);

      if (!isDeepStrictEqual(firstResult, secondResult)) {
        throw createUpcastError(
          "EVENT_UPCAST_NON_DETERMINISTIC",
          `Upcaster for ${currentEvent.eventType} v${version} returned different results for the same event`,
          {
            sourceEvent: event,
            eventType: currentEvent.eventType,
            fromVersion: version,
            toVersion: upcaster.toVersion,
            targetSchemaVersion: resolvedTargetVersion,
            reason: "OUTPUT_MISMATCH",
          }
        );
      }

      currentEvent = firstResult;
      version = upcaster.toVersion;
    }

    return Object.freeze(currentEvent);
  }

  hasUpcaster(eventType, fromVersion) {
    return this.#upcasters.has(`${eventType}:${fromVersion}`);
  }
}

module.exports = {
  EventUpcasterRegistry,
};
