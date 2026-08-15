# Rollback Engine

Rollback Engine is a small event-sourced backend that explores the hard parts of
reliable command processing: deterministic state reconstruction, saga
compensation, idempotent retries, optimistic concurrency, uncertain commits and
repairable read models.

The project deliberately uses one Node.js process and in-memory adapters. Its
purpose is not to simulate a large platform, but to make correctness boundaries
explicit, executable and reviewable.

## The problem

A checkout request can fail after some work was committed, an HTTP response can
be lost, or a materialized view can become unavailable while the event stream
remains intact. A safe system must answer three questions without guessing:

1. Which domain facts were committed?
2. Can the command be retried without producing duplicate facts?
3. Can current state be reconstructed exclusively from retained history?

Rollback Engine treats the Event Store as the answer to the first and third
questions. The Command Store records idempotency outcomes, but reconciliation
always checks those records against the authoritative event history.

## Architecture in five minutes

```text
HTTP routes
    │ transport validation, headers, status codes
    ▼
RollbackEngine ───────────── CommandExecutionCoordinator
    │ domain orchestration          │ idempotency and commit semantics
    │                               │
    ├── Checkout Saga               ├── Command Store
    ├── Projection                  └── Event Store command index
    ├── Replay / Recovery
    ├── Snapshot validation
    └── Timeline
            │
            ▼
Store contracts ── In-memory adapters
```

| Component | Responsibility |
| --- | --- |
| Domain events | Immutable business facts with a separate technical metadata envelope |
| Projection | Deterministic domain state machine and transition authority |
| Checkout saga | Forward steps and state-aware reverse compensation |
| `RollbackEngine` | Application use cases, event creation, replay, recovery, snapshots and view repair |
| `CommandExecutionCoordinator` | Reservation, idempotency, command/event reconciliation and commit-aware errors |
| Event Store | Append-only source of truth, aggregate ordering and command-ID index |
| Command Store | Process-local command outcome cache; never authoritative for domain state |
| State Repository | Disposable materialized view used by optimized reads |
| Snapshot Store | Disposable replay optimization |
| HTTP layer | Transport validation and stable public contracts only |

The implementation does not introduce a generic repository hierarchy or a
service per method. `RollbackEngine` and `CommandExecutionCoordinator` are kept
separate because the domain/replay boundary and the technical command boundary
have different failure semantics.

## Core guarantees

```text
Authoritative State == Full Replay State
Full Replay State == Valid Snapshot State + Events after Snapshot Version
same command key + same normalized payload == no duplicate execution
```

The implementation enforces the following consequences:

- Aggregate sequences are contiguous and are the canonical order. Timestamps
  may be equal, but cannot move backwards within an aggregate.
- The projection rejects invalid lifecycle transitions, mismatched inventory
  data, incorrect compensation order and deletion before compensation.
- Event append uses `expectedVersion`; a stale writer cannot extend a stream
  based on an obsolete state assumption.
- Recovery rebuilds only the materialized view and emits no domain events.
- Snapshots are validated against their event-stream prefix. A missing,
  malformed, ahead or inconsistent snapshot falls back to full replay.
- Snapshot persistence is outside the domain commit boundary. Failure produces
  a warning and a technical diagnostic, without changing command success.
- If a view write fails after append, the engine immediately rebuilds it from
  events. If repair also fails, the public error explicitly reports the event
  as committed.
- Recognized checkout failures are compensated. Infrastructure and programming
  failures are propagated instead of triggering blind compensation.

### Exact read-state contract

`GET /replay-state/:orderId` is authoritative because it rebuilds state from
the Event Store. `GET /orders` reads the State Repository and is an optimized
materialized-view read. It does not validate the view on every request.

```text
Materialized View == Full Replay State
after every command whose view update or synchronous repair succeeds
```

After the explicit `EVENT_COMMITTED_VIEW_REPAIR_FAILED` error, the view may be
stale or unavailable while authoritative replay remains correct. A subsequent
`POST /replay-restore/:orderId` repairs the view from retained events.

## Checkout flows

A successful checkout appends:

```text
1  ORDER_CREATED
2  INVENTORY_RESERVED
3  PAYMENT_CHARGED
```

A recognized failure after payment appends the forward facts and compensates in
reverse order:

```text
1  ORDER_CREATED
2  INVENTORY_RESERVED
3  PAYMENT_CHARGED
4  PAYMENT_REFUNDED
5  INVENTORY_RELEASED
6  ORDER_ROLLED_BACK
```

There is intentionally no `COMMAND_FAILED` domain event. A technical command
failure is not a business fact and is reported through the error contract and
the diagnostic reporter.

## API contract

The complete OpenAPI 3.1 contract is stored in [`openapi.json`](./openapi.json)
and served at `GET /openapi.json`. It documents request bodies, trace headers,
response schemas, retry semantics and complete success/compensation examples.
No Swagger UI or OpenAPI runtime dependency is required.

| Method | Path | Contract |
| --- | --- | --- |
| `GET` | `/` | Service status |
| `GET` | `/openapi.json` | Machine-readable API contract |
| `POST` | `/order` | Create an order |
| `DELETE` | `/order/:id` | Delete an inactive or compensated order |
| `POST` | `/checkout` | Execute checkout, including recognized compensation flows |
| `GET` | `/orders` | Read the optimized materialized order list |
| `GET` | `/history` | Read all committed events in global append order |
| `GET` | `/timeline/:orderId` | Read a compact event/trace timeline |
| `GET` | `/replay-state/:orderId` | Authoritative full replay |
| `GET` | `/snapshot/:orderId` | Inspect the current snapshot optimization |
| `POST` | `/replay-restore/:orderId` | Repair a materialized view from events |
| `GET` | `/state-at/:orderId/:timestamp` | Replay a contiguous prefix at a point in time |

### Write headers

| Header | Meaning |
| --- | --- |
| `Idempotency-Key` | Stable command identity. Reuse only for the same command type and normalized payload. |
| `X-Correlation-Id` | Identifier shared by related commands in one business flow. |
| `X-Causation-Id` | Cause of the first event; later events point to the preceding event. |

All headers are optional, but only a request with `Idempotency-Key` can replay a
stored outcome after a lost HTTP response. A supplied key is echoed in the
response header once command context has been accepted.

Example:

```bash
curl --request POST http://localhost:3000/checkout \
  --header 'content-type: application/json' \
  --header 'Idempotency-Key: checkout-123' \
  --header 'X-Correlation-Id: customer-flow-456' \
  --header 'X-Causation-Id: request-789' \
  --data '{"item":"Pizza","quantity":1,"amount":100}'
```

Repeating that request with the same key and normalized payload returns the
stored result without allocating IDs or appending events. Reusing the key with
a different payload returns `IDEMPOTENCY_KEY_CONFLICT`.

### Public error and retry contract

Every HTTP error has the same envelope. Internal causes and stack traces are
never exposed.

```json
{
  "error": {
    "code": "EVENT_APPEND_COMMIT_UNKNOWN",
    "category": "technical",
    "message": "The event store did not confirm whether the event was committed.",
    "eventCommitted": null,
    "retrySafe": false,
    "retryAction": "RECONCILE_SAME_KEY",
    "commandId": "checkout-123",
    "aggregateId": 42,
    "eventId": "event-123"
  }
}
```

`eventCommitted` is tri-state:

- `true`: at least one event is proven committed. Do not repeat the command.
- `false`: no event was committed for this failed attempt.
- `null`: the Event Store outcome could not be proven. Never guess from the
  HTTP status.

`retrySafe` means that re-execution cannot duplicate a known commit; it does
not promise that the unchanged request will succeed. `retryAction` is the
client instruction:

| Situation | Typical code | Required action |
| --- | --- | --- |
| Invalid request | `VALIDATION_ERROR` | `FIX_REQUEST` |
| Domain requires compensation | `COMPENSATION_REQUIRED` | `COMPENSATE_THEN_RETRY` |
| Same key, different command or payload | `IDEMPOTENCY_KEY_CONFLICT` | `USE_NEW_KEY` |
| Existing command still processing | `COMMAND_IN_PROGRESS` | `WAIT_AND_RETRY_SAME_KEY` |
| Proven pre-commit keyed failure | original technical code | `RETRY_SAME_KEY` |
| Proven pre-commit unkeyed conflict | `OPTIMISTIC_CONCURRENCY_CONFLICT` | `RETRY_COMMAND` |
| Stored deterministic rejection | original domain code | `REPLAY_SAME_KEY` |
| Unknown commit or reconciliation lookup failure | `EVENT_APPEND_COMMIT_UNKNOWN`, `COMMAND_RECONCILIATION_FAILED` | `RECONCILE_SAME_KEY` |
| Known partial commit, inconsistent history or failed view repair | corresponding stable code | `MANUAL_RESOLUTION_REQUIRED` |
| Snapshot warning after success | `SNAPSHOT_SAVE_FAILED` | `DO_NOT_RETRY_COMMAND` |

Clients must preserve and reuse the same idempotency key for every reconciliation
attempt. A new key represents a new command.

## Event metadata and debugging timeline

Every event contains business payload and a separate immutable metadata
envelope:

```json
{
  "schemaVersion": 1,
  "commandId": "checkout-123",
  "correlationId": "customer-flow-456",
  "causationId": "event-previous"
}
```

`GET /timeline/:orderId` derives a compact trace exclusively from committed
events. It does not project state, duplicate domain rules or create new events.

```json
{
  "aggregateId": 42,
  "version": 3,
  "eventCount": 3,
  "commandIds": ["checkout-123"],
  "correlationIds": ["customer-flow-456"],
  "entries": [
    {
      "sequence": 1,
      "eventId": "event-1",
      "eventType": "ORDER_CREATED",
      "timestamp": "2026-08-15T10:00:00.000Z",
      "schemaVersion": 1,
      "commandId": "checkout-123",
      "correlationId": "customer-flow-456",
      "causationId": "request-789"
    }
  ]
}
```

The endpoint shows domain facts and their causal chain. Technical failures do
not appear as synthetic domain events; they belong to diagnostics.

## Structured technical diagnostics

`RollbackEngine` accepts an optional reporter callback:

```js
const engine = new RollbackEngine({
  diagnosticReporter: (diagnostic) => console.log(JSON.stringify(diagnostic)),
});
```

Diagnostics are immutable records with stable `type` and `status` values,
normalized `occurredAt` and available command, aggregate and event identifiers.
Current boundary outcomes are:

| Type | Statuses |
| --- | --- |
| `COMMAND_RECONCILIATION` | `LOOKUP_FAILED` |
| `EVENT_APPEND` | `COMMIT_UNKNOWN`, `COMMIT_CONFIRMED_AFTER_ERROR` |
| `MATERIALIZED_VIEW_REPAIR` | `REPAIRED`, `REPAIR_FAILED` |
| `SNAPSHOT_SAVE` | `SAVE_FAILED` |

Example:

```json
{
  "type": "MATERIALIZED_VIEW_REPAIR",
  "status": "REPAIRED",
  "occurredAt": "2026-08-15T10:00:00.000Z",
  "commandId": "checkout-123",
  "aggregateId": 42,
  "eventId": "event-3"
}
```

Reporting is best-effort and outside the command boundary. A reporter throw or
rejected promise cannot change a successful command, committed event or public
warning. The engine intentionally provides no log framework, persistence,
delivery retry or sensitive internal error details; production composition can
forward these records to its existing observability system.

## Idempotency and command/event reconciliation

For each key, the Command Store retains command type, normalized payload,
status, aggregate and event range, plus the completed result or stable failure
descriptor. Before replaying a stored result, the coordinator checks the range
against the Event Store command-ID index.

- `completed` returns its stored result only when the recorded range matches.
- `failed` replays deterministic or committed failures without executing again.
- `processing` with no events remains in progress; it has no unsafe timeout.
- An unknown append is reconciled with the same key. Events found means manual
  resolution; no events found allows controlled re-execution.
- Non-contiguous command events or events spanning aggregates are rejected as
  `COMMAND_EVENT_HISTORY_INCONSISTENT`.

The Command Store is therefore an idempotency record, not a second source of
domain truth.

## Snapshots, recovery and time travel

Snapshot state is accepted only when its aggregate, version, timestamp and
projected event prefix agree. Replay then applies the suffix after the snapshot
version. Otherwise it uses full replay.

Time travel also respects sequence order. It selects the longest contiguous
event prefix whose timestamps are not later than the requested instant. Events
with equal timestamps become visible together; sequence replay is the precise
ordering tool.

Automatic snapshots after checkout and deletion are best-effort. A failure
returns this warning inside the successful command result:

```json
{
  "code": "SNAPSHOT_SAVE_FAILED",
  "category": "technical",
  "eventCommitted": true,
  "retrySafe": false,
  "retryAction": "DO_NOT_RETRY_COMMAND",
  "aggregateId": 42
}
```

## Persistence boundary

Required adapter capabilities are declared in
`src/application/storeContracts.js`; reusable semantic suites live in
`tests/support/storeContractSuites.js`.

The engine provides two storage implementations:
1. **InMemory Adapters**: Fast, isolated, ideal for unit testing.
2. **SQLite WAL Adapters (`src/infrastructure/sqlite/`)**: Persistent, file-backed or in-memory, using native `node:sqlite` (`DatabaseSync`).

| Store | Critical semantics |
| --- | --- |
| Event Store | Atomic expected-version check and append, global event-ID uniqueness, explicit `command_id` index, aggregate ordering, read-after-write consistency |
| Command Store | Atomic absent-key reservation, valid status transitions, atomic result/error persistence and defensive values |
| Snapshot Store | Atomic version comparison and replacement, equivalent same-version idempotency |
| State Repository | Atomic whole-state save/replace and defensive materialized values |

### Storage Configuration

By default, the server runs with in-memory adapters. To run with persistent SQLite storage:

```bash
# In-Memory (default)
STORAGE=memory npm start

# Persistent SQLite WAL Storage
STORAGE=sqlite DB_PATH=./data/rollback.db npm start
```

## Run and verify

```bash
npm install
npm start
```

The server listens on `http://localhost:3000`.

```bash
npm test
npm run check
git diff --check
```

The tests cover domain transitions, compensation order, replay equivalence,
snapshot fallback, time-travel prefixes, commit uncertainty, view repair,
idempotency reconciliation, optimistic concurrency, store contracts (both In-Memory and SQLite),
file-backed restart durability across separate Node processes, lost ACK reconciliation,
multi-process optimistic concurrency, the OpenAPI route set, and representative API flows.

## Deliberate limits

- **Synchronous SQLite I/O**: SQLite runs synchronously via `node:sqlite` `DatabaseSync`. While this guarantees zero async race conditions and preserves existing contracts, synchronous I/O can block the Node.js event loop under high concurrent loads.
- **`processing + 0 events` boundary**: A command found in `processing` state without committed events after a crash is NOT automatically stolen or reset upon restart. It remains `COMMAND_IN_PROGRESS` (`WAIT_AND_RETRY_SAME_KEY`) until distributed lease/fencing logic is introduced.
- **Single-Machine Storage**: SQLite WAL provides single-machine durability, not distributed multi-node consensus.
- **No Multi-Machine Leases / Fencing Tokens**: Command coordination is local; distributed worker fencing is reserved for future phases.
- One command event range currently belongs to one aggregate.
- Aggregate, reservation and payment IDs are allocated in process.
- Requests without `Idempotency-Key` cannot replay an outcome after a lost HTTP response.
- OpenAPI is a reviewed static contract, not runtime request/response validation.

## Sensible next steps

1. **Interactive Chaos & Time-Travel Cockpit**: A lightweight dashboard visualizing the persistent event log, saga compensations, and live fault injection.
2. **Deterministic Chaos Invariant Fuzzing**: Continuous fault-injection testing running thousands of interleaved operations against the persistent store.
3. **Distributed Command Leases & Fencing Tokens**: Solving multi-worker crash recovery for unresolved processing commands.
