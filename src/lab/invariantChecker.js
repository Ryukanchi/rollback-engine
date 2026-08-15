function checkInvariants({
  events = [],
  authoritativeState = null,
  materializedState = null,
  replayedState = null,
  duplicateEventsCount = 0,
} = {}) {
  // Invariant 1: Sequences must be strictly contiguous (1, 2, ..., N)
  let sequenceContiguous = true;
  let sequenceDetails = "No events";

  if (events.length > 0) {
    for (let i = 0; i < events.length; i++) {
      if (events[i].sequence !== i + 1) {
        sequenceContiguous = false;
        sequenceDetails = `Gap/Mismatch detected: expected sequence ${i + 1}, found ${events[i].sequence}`;
        break;
      }
    }
    if (sequenceContiguous) {
      sequenceDetails = `Sequences 1..${events.length} are strictly contiguous`;
    }
  }

  // Invariant 2: Unique Event IDs
  const eventIds = new Set();
  let eventIdsUnique = true;
  let eventIdDetails = "0 events checked";

  if (events.length > 0) {
    for (const evt of events) {
      if (eventIds.has(evt.eventId)) {
        eventIdsUnique = false;
        eventIdDetails = `Duplicate eventId detected: ${evt.eventId}`;
        break;
      }
      eventIds.add(evt.eventId);
    }
    if (eventIdsUnique) {
      eventIdDetails = `All ${events.length} event IDs are globally unique`;
    }
  }

  // Invariant 3: Timestamps monotonically non-decreasing
  let timestampsMonotonic = true;
  let timestampDetails = "No timestamps to verify";

  if (events.length > 1) {
    for (let i = 1; i < events.length; i++) {
      const prev = new Date(events[i - 1].timestamp).getTime();
      const curr = new Date(events[i].timestamp).getTime();
      if (curr < prev) {
        timestampsMonotonic = false;
        timestampDetails = `Timestamp regression: sequence ${events[i].sequence} (${events[i].timestamp}) < sequence ${events[i - 1].sequence} (${events[i - 1].timestamp})`;
        break;
      }
    }
    if (timestampsMonotonic) {
      timestampDetails = `All ${events.length} timestamps are monotonically non-decreasing`;
    }
  } else if (events.length === 1) {
    timestampDetails = "Single timestamp valid";
  }

  // Invariant 4: Replay State == Authoritative State
  let replayAuthoritativeMatch = false;
  let replayDetails = "No state to compare";

  if (authoritativeState && replayedState) {
    const authJson = JSON.stringify(authoritativeState);
    const replayJson = JSON.stringify(replayedState);
    replayAuthoritativeMatch = authJson === replayJson;
    replayDetails = replayAuthoritativeMatch
      ? "Replayed state matches authoritative event projection"
      : "Replayed state diverged from authoritative state";
  }

  // Invariant 5: Materialized View Synchronized
  let viewSynchronized = false;
  let viewDetails = "No materialized view";

  if (authoritativeState && materializedState) {
    const authJson = JSON.stringify(authoritativeState);
    const matJson = JSON.stringify(materializedState);
    viewSynchronized = authJson === matJson;
    viewDetails = viewSynchronized
      ? "Materialized view is synchronized with authoritative log"
      : `Read model drift detected: materialized version ${materializedState.version} vs authoritative version ${authoritativeState.version}`;
  } else if (!materializedState && authoritativeState) {
    viewSynchronized = false;
    viewDetails = "Materialized view missing (unpopulated or cleared)";
  }

  // Invariant 6: Duplicate Retry Events
  const duplicateRetrySafe = duplicateEventsCount === 0;
  const duplicateDetails = duplicateRetrySafe
    ? "0 duplicate events generated across command retries"
    : `${duplicateEventsCount} duplicate event(s) detected during retry`;

  return {
    sequenceContiguous: {
      passed: sequenceContiguous,
      label: "Contiguous Sequences",
      details: sequenceDetails,
    },
    eventIdsUnique: {
      passed: eventIdsUnique,
      label: "Unique Event IDs",
      details: eventIdDetails,
    },
    timestampsMonotonic: {
      passed: timestampsMonotonic,
      label: "Monotonic Timestamps",
      details: timestampDetails,
    },
    replayAuthoritativeMatch: {
      passed: replayAuthoritativeMatch,
      label: "Replay ≡ Authoritative State",
      details: replayDetails,
    },
    viewSynchronized: {
      passed: viewSynchronized,
      label: "Materialized View Synchronized",
      details: viewDetails,
    },
    duplicateRetryEvents: {
      passed: duplicateRetrySafe,
      label: "Duplicate-Safe Retries",
      details: duplicateDetails,
    },
  };
}

module.exports = {
  checkInvariants,
};
