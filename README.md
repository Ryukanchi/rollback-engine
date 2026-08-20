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

### Upcaster safety contract

Upcasters translate one stored event into a newer representation; they do not
produce events or correct history. Every registered edge advances exactly one
schema version. After every edge, the registry preserves `eventId`,
`aggregateId`, `sequence`, `timestamp`, `eventType`, `commandId`,
`correlationId` and `causationId`, owns the resulting `schemaVersion`, and
validates the complete event before returning it. A missing edge, invalid
output or identity change fails closed before replay, snapshots, materialized
view repair or command reconciliation can consume the result.

Each edge is evaluated twice against independent immutable inputs. Different
outputs reject common clock-, random- and mutable-state dependencies before the
first result can reach projection. This is a detection aid, not a proof that
arbitrary JavaScript is pure or that a deterministic payload migration is
semantically correct. Migration review and stable golden fixtures remain part
of the published schema contract; current configuration and external systems
must never be migration inputs.

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
- `processing` with valid lease remains in progress (`COMMAND_IN_PROGRESS`).
- `processing` with expired lease and 0 events allows safe atomic takeover by another worker with an incremented fencing token.
- `processing` with $\ge 1$ committed events cannot be taken over; post-commit reconciliation handles it safely without re-execution.
- An unknown append is reconciled with the same key. Events found means manual
  resolution; no events found allows controlled re-execution.
- Non-contiguous command events or events spanning aggregates are rejected as
  `COMMAND_EVENT_HISTORY_INCONSISTENT`.

The Command Store is therefore an idempotency and coordination record, not a second source of
domain truth.

## Command Lease Ownership, Safe Takeover & Fencing Tokens

To close the crash recovery gap where a worker crashes before appending its first event (`processing + 0 committed events`), the engine implements multi-process **Lease Ownership** and **Fencing Tokens**.

```text
Worker A (PID 101)                 Shared SQLite Storage                  Worker B (PID 102)
       │                                     │                                    │
       ├─── Reserve command-1 ──────────────►│ (lease_owner: Worker A,            │
       │    (TTL: 1000ms)                    │  lease_token: 1,                   │
       │                                     │  lease_expires_at: T+1000)         │
       │                                     │                                    │
       │   [Worker A pauses / GC stall]      │                                    │
       │   (Time advances past T+1000)       │                                    │
       │                                     │◄── Takeover command-1 ─────────────┤
       │                                     │    (Atomic BEGIN IMMEDIATE:        │
       │                                     │     lease_token = 2,               │
       │                                     │     lease_owner: Worker B)         │
       │                                     │                                    │
       │                                     │◄── Append Event 1..3 (token: 2) ───┤
       │                                     │    [Events Committed & Cached]     │
       │                                     │                                    │
       │   [Worker A wakes up as Zombie]     │                                    │
       ├─── Append Event 1 (stale token: 1) ─►│ Atomic Fencing Check in           │
       │                                     │ SqliteEventStore.append():         │
       │                                     │ token 1 !== current token 2        │
       │◄── ROLLBACK & Reject ───────────────┤                                    │
       │    FENCING_TOKEN_STALE              │                                    │
       ▼                                     ▼                                    ▼
Authoritative Event Store remains 100% pure — Zombie Worker cannot commit stale facts.
```

### Core Lease & Fencing Semantics

1. **Monotonic Fencing Tokens**: Initial command reservation allocates `lease_token = 1`. Each successful takeover strictly increments the token ($Token_{new} = Token_{old} + 1$).
2. **Atomic In-Transaction Fencing Enforcement**: In `SqliteEventStore.append()`, the fencing check, expectedVersion check, and event insertion occur atomically within the **same `BEGIN IMMEDIATE ... COMMIT` transaction**. TOCTOU races between application-level lease validation and SQLite database commit are technically impossible.
3. **Mandatory Fencing Token for Leased Commands**: When a command record exists in `status === 'processing'`, `EventStore.append()` requires a valid `fencingToken`. Omitting or passing `null`/`undefined` fencingToken is strictly rejected with `FENCING_TOKEN_REQUIRED` without committing events.
4. **Authoritative Event Store Boundary for Takeover**: `takeOverExpired()` directly queries the authoritative `events` table inside its `BEGIN IMMEDIATE` write transaction. Even if command bookkeeping lags behind (`commands.event_range` not yet recorded), takeover is strictly blocked with `HAS_EVENTS`.
5. **Deterministic Race Safety Outcomes**:
   - **Append Wins**: Event committed in `events` table. Subsequent takeover attempts see $\ge 1$ committed events and abort with `HAS_EVENTS`.
   - **Takeover Wins**: Lease token incremented to $N+1$. Stale worker's subsequent append presents token $N$ and is rejected with `FENCING_TOKEN_STALE` while the command is still `processing`.
6. **Clock Semantics & Hygiene**: Lease expiration uses `Date.now()` representing **wall-clock epoch milliseconds** (NOT a monotonic hardware clock). It is subject to operating system NTP adjustments and host clock steps. Systems relying on leases must configure lease TTLs with appropriate safety margins against anticipated clock skew.
7. **Defensive Mutation Guards**: `recordEvent()`, `complete()`, and `fail()` require and validate the worker's active `fencingToken`, preventing stale workers from modifying command status or releasing reservations.
8. **Schema Migration via `PRAGMA user_version`**: Seamlessly migrates existing SQLite databases from v1 to v2 by adding `lease_owner`, `lease_token`, and `lease_expires_at` columns with automatic indexing.

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
| Event Store | Atomic expected-version check, transactional lease fencing check, global event-ID uniqueness, explicit `command_id` index, aggregate ordering, read-after-write consistency |
| Command Store | Atomic absent-key reservation, lease ownership, atomic expired takeover with monotonic fencing tokens, valid status transitions, atomic result/error persistence |
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

## Interactive Reliability Lab

The project includes an interactive engineering cockpit to inspect real event streams, replay historical sequences, inject faults, test self-healing, and verify post-crash reconciliation.

### Starting the Lab

```bash
# Start server with Lab Mode enabled
npm run lab

# Alternatively:
LAB_MODE=1 npm start
```

Open `http://localhost:3000/lab` in your browser.

### Safety & Isolation Boundary

- **`LAB_MODE` Guard**: When `LAB_MODE` is disabled (default in production), `/lab` and destructive scenario APIs are completely disabled (HTTP 404), ensuring standard API endpoints are never exposed to test faults.
- **Disposable SQLite Databases**: Every scenario run operates in a dedicated, isolated temporary SQLite database file (e.g. `rollback-lab-<id>.db`). The primary application database is never touched.

### Demonstrable Scenarios

1. **Successful Checkout Saga**: Executes standard 3-step forward saga (`ORDER_CREATED`, `INVENTORY_RESERVED`, `PAYMENT_CHARGED`), committing contiguous sequences and verifying state projection.
2. **Compensation After Payment**: Injects fault at `simulateFailureAt: 'after_payment'`, demonstrating automatic reverse compensation (`PAYMENT_REFUNDED`, `INVENTORY_RELEASED`, `ORDER_ROLLED_BACK`) to safely roll back state.
3. **Sequence Time Travel & State Diff**: Interactive sequence scrubber allowing deterministic state inspection at any point in history (`replayAtSequence()`) with highlighted delta diffs ($\Delta$ from $N-1$).
4. **Logical Read-Model Drift & Self-Healing**: Deliberately mutates the SQLite materialized cache row. The UI displays the drift against the authoritative event log and provides a button to trigger authoritative self-healing.
5. **Post-Commit Reconciliation**: Simulates a process crash after Event Store append but before completion ACK. On retry with the same `Idempotency-Key`, existing events are reconciled via indexed `command_id` without executing duplicate side-effects.
6. **`processing + 0 events` Boundary**: Demonstrates the safety boundary refusing automatic takeover while a lease is active, returning `COMMAND_IN_PROGRESS`.
7. **Lease Recovery & Zombie Fencing**: Demonstrates safe takeover of an expired lease by Worker B ($Token \rightarrow 2$) and atomic rejection of Worker A's stale append with `FENCING_TOKEN_STALE`.
8. **Process Restart Durability**: Spawns two real independent OS child processes. Process A commits the event stream to disk and terminates; Process B opens the database file from scratch and reconstructs the exact domain state via replay.

## Deterministic Chaos & Invariant Fuzzing

The repository includes a deterministic chaos and invariant fuzzing harness designed to explore thousands of interleaved operations, real saga fault injections, command retries, logical read-model view drifts, snapshot corruptions, lease takeovers, zombie fencing, and connection reopen cycles against both In-Memory and persistent SQLite storage adapters.

### Design Principles

- **Seed-Deterministic Operation Generation**: Built with a custom 32-bit Mulberry32 PRNG. Zero calls to unseeded `Math.random()`. Every execution sequence and logical trace is 100% reproducible by providing the exact seed.
- **Real Engine & Real Stores**: The harness exercises the actual `RollbackEngine`, `CommandExecutionCoordinator`, projections, sagas, leases, fencing checks, and SQLite/in-memory store contracts without mock semantics.
- **Real Saga Fault Injection Coverage**: Directly exercises all supported saga failure points (`after_order`, `after_inventory`, `after_payment`) and validates that partial and full reverse compensation streams are executed in strict inverse order.
- **Granular Single-Iteration Reproduction**: Any discovered failure or invariant violation can be reproduced directly on its specific iteration index:
  ```bash
  npm run chaos -- --seed=482971 --iteration=7312 --profile=memory
  ```
- **Per-Invariant Coverage Counting**: Aggregates and reports the exact number of times each invariant and failure path was evaluated during a campaign.

### Invariant Catalog (21 Checked Invariants)

| Invariant | Description | Verification Method |
| :--- | :--- | :--- |
| **Replay Stability** | Pure repeatability of event replay | Asserts that repeated `replay(agg)` on the same immutable event stream produces identical state |
| **Snapshot Equivalence** | Snapshot + suffix equals full replay | Compares `replayFromSnapshot(agg)` against `replay(agg)` |
| **Event Sequence Contiguity** | $1..N$ contiguous sequence without gaps | Verifies $Sequence_i == i + 1$ for all aggregate streams |
| **Global Event ID Uniqueness** | Unique event IDs across all aggregates | Tracks global set of event IDs across store |
| **Timestamp Monotonicity** | Non-decreasing timestamps per stream | Asserts $Timestamp_n \ge Timestamp_{n-1}$ |
| **Projection Determinism** | Pure state machine behavior | Projects identical event stream multiple times and asserts equality |
| **Materialized View Consistency** | Synchronized derived read model | Compares `getState('materialized')` with `replay(agg)` (skips intentional unrepaired drift) |
| **Idempotent Retry Safety** | Re-execution produces $\Delta events = 0$ | Verifies stable outcome without duplicate event appends |
| **Idempotency Conflict Detection** | Same ID with altered payload rejected | Asserts conflict error without aggregate state mutation |
| **Post-Commit Retry Safety** | Interrupted commit cannot duplicate side effects | Verifies reconciliation prevents re-execution |
| **`processing + 0` Boundary** | Uncompleted commands without events not stolen | Confirms `COMMAND_IN_PROGRESS` while lease is unexpired |
| **Lease Ownership Consistency** | Monotonic tokens and clean lease release | Verifies `leaseToken \ge 1` and `leaseOwner === null` upon completion/failure |
| **Fencing Safety** | Stale workers cannot mutate Event Store | Validates atomic rejection with `FENCING_TOKEN_STALE` |
| **Compensation Ordering** | Correct forward and inverse saga steps | Asserts refund precedes release and rollback in saga history depending on completed forward steps |
| **No Impossible Final State** | Domain state machine invariants | Prevents invalid states (e.g. `rolled_back` with active charges or `deleted` with reservations) |
| **Commit Boundary** | Persistence of committed events is final | Derived store errors or stale attempts do not remove committed event log entries |
| **Command Range Consistency** | Recorded range matches Event Store | Validates `firstSequence` and `lastSequence` against store |
| **Aggregate Isolation** | Streams do not leak across aggregate IDs | Replay of Aggregate A depends exclusively on Events of Aggregate A |
| **Time Travel Prefix** | `replayAtSequence(N)` equals prefix projection | Projects $Events[0..N]$ and compares with time-travel query |
| **Defensive Copies** | External mutations do not corrupt store | Validates immutability / defensive copies on retrieved objects |
| **Schema Upcasting** | Historical events read compatibly | Verifies `EventUpcasterRegistry` transformation without store mutation |

### Running Chaos Campaigns

```bash
# Run standard campaign (5,000 in-memory + 250 persistent SQLite iterations)
npm run chaos

# Run smoke test (100 in-memory + 10 SQLite iterations)
npm run chaos -- --campaign=smoke

# Run high-volume in-memory campaign with specific seed
npm run chaos -- --profile=memory --iterations=10000 --seed=482971

# Run persistent SQLite campaign with specific seed (with connection reopen cycles)
npm run chaos -- --profile=sqlite --iterations=500 --seed=6006

# Reproduce a specific single iteration
npm run chaos -- --seed=482971 --iteration=7312 --profile=memory
```

### Claim Hygiene

- **Single-Machine Multi-Process Coordination vs. Distributed Multi-Node Consensus**: Command Leases and Fencing Tokens coordinate independent OS processes accessing shared SQLite storage on a single host. This prevents stale workers from corrupting the authoritative event log. It is an ordering and stale-writer protection mechanism, not distributed multi-node consensus (Raft/Paxos) across network partitions.
- **Empirical Fuzzing vs. Formal Proof**: The chaos harness provides strong empirical verification for the system's core invariants under local interleaving and single-machine crash/reopen boundaries. It is an empirical testing tool, not a formal mathematical verification.
- **Partial-Commit Reconciliation Takes Precedence**: The system deliberately does not allow lease takeover when $\ge 1$ events have been committed. Expired lease takeover is strictly constrained to `processing + 0 events`.

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

The 248 tests cover domain transitions, compensation order, replay equivalence,
snapshot fallback, time-travel prefixes, commit uncertainty, view repair,
idempotency reconciliation, optimistic concurrency, store contracts (both In-Memory and SQLite),
file-backed restart durability across separate Node processes, lost ACK reconciliation,
multi-process optimistic concurrency, multi-process zombie worker fencing, schema migration, the `LAB_MODE` security boundary, scenario runners,
deterministic chaos regression tests, reproduction tests, and representative API flows.

## Deliberate limits

- **Synchronous SQLite I/O**: SQLite runs synchronously via `node:sqlite` `DatabaseSync`. While this guarantees zero async race conditions and preserves existing contracts, synchronous I/O can block the Node.js event loop under high concurrent loads.
- **Single-Host Shared Storage**: Multi-process leases and fencing tokens coordinate processes on a shared storage medium; multi-node network partitions and distributed clustering are not modeled.
- One command event range currently belongs to one aggregate.
- Aggregate, reservation and payment IDs are allocated in process.
- Requests without `Idempotency-Key` cannot replay an outcome after a lost HTTP response.
- OpenAPI is a reviewed static contract, not runtime request/response validation.

## Sensible next steps

1. **Async Storage Adapter Boundary**: Introducing non-blocking async store adapters with connection pooling for high-throughput multi-tenant deployments.
2. **Distributed Cluster Consensus**: Exploring Raft/Paxos-based multi-node replication and distributed state machine replication.
3. **Cross-Aggregate Saga Coordination**: Supporting multi-aggregate transactional workflows.
