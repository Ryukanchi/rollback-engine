function unique(values) {
  return [...new Set(values)];
}

function buildEventTimeline(events) {
  if (!Array.isArray(events)) {
    throw new TypeError("events must be an array");
  }

  if (events.length === 0) {
    return null;
  }

  const aggregateId = events[0].aggregateId;
  const entries = events.map((event) => ({
    sequence: event.sequence,
    eventId: event.eventId,
    eventType: event.eventType,
    timestamp: event.timestamp,
    schemaVersion: event.metadata.schemaVersion,
    commandId: event.metadata.commandId,
    correlationId: event.metadata.correlationId,
    causationId: event.metadata.causationId,
  }));

  return {
    aggregateId,
    version: entries[entries.length - 1].sequence,
    eventCount: entries.length,
    commandIds: unique(entries.map((entry) => entry.commandId)),
    correlationIds: unique(entries.map((entry) => entry.correlationId)),
    entries,
  };
}

module.exports = {
  buildEventTimeline,
};
