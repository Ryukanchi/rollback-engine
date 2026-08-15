const { assertDomainEvent, assertDomainFact } = require("./events");

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

class EventUpcasterRegistry {
  #upcasters = new Map();

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
      throw new Error("toVersion must be strictly greater than fromVersion");
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

    return this;
  }

  upcast(event, targetSchemaVersion) {
    if (!event || typeof event !== "object") {
      throw new TypeError("event must be an object");
    }

    const currentVersion = event.metadata?.schemaVersion ?? 1;

    if (targetSchemaVersion !== undefined) {
      assertPositiveInteger(targetSchemaVersion, "targetSchemaVersion");
    }

    let currentEvent = event;
    let version = currentVersion;

    while (targetSchemaVersion === undefined || version < targetSchemaVersion) {
      const key = `${currentEvent.eventType}:${version}`;
      const upcaster = this.#upcasters.get(key);

      if (!upcaster) {
        break;
      }

      const transformed = upcaster.upcast(currentEvent);

      if (!transformed || typeof transformed !== "object") {
        throw new Error(
          `Upcaster for ${currentEvent.eventType} v${version} returned invalid event`
        );
      }

      const nextMetadata = {
        ...transformed.metadata,
        schemaVersion: upcaster.toVersion,
      };

      currentEvent = {
        ...transformed,
        metadata: Object.freeze(nextMetadata),
      };

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
