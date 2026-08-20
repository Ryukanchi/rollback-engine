# Rollback Engine

> An event-sourced reliability lab for commands that may have succeeded even
> when the caller never received an answer.

Rollback Engine does not rewind time. It keeps the facts that already happened,
reconstructs state from those facts, and uses explicit compensation when a
multi-step operation cannot finish.

The project is intentionally small enough to review in one repository, but it
focuses on problems that show up in serious distributed systems: lost
acknowledgements, partial execution, retries, stale workers, corrupted read
models, schema evolution, and the uncomfortable state called “we do not know
whether the write committed.”

Current reliability checkpoint:

- **528 automated tests**
- in-memory and SQLite reference adapters
- deterministic replay, recovery, saga compensation, idempotency, leases and
  fencing
- completed authority-containment work for A-1, F-2, F-3, F-4 and F-6

## Why this exists

Imagine a checkout flow:

1. an order is created;
2. inventory is reserved;
3. a payment is charged;
4. the worker times out before the response reaches the client.

Did the payment happen? Can the request be retried? Is the command still owned
by the old worker? Is the row shown by the read API current, stale, or simply
wrong?

A normal CRUD application is great when one database transaction contains the
whole truth. It becomes less helpful when work crosses time, processes, stores,
or external APIs. Updating a `status` column tells us the latest value someone
wrote; it does not necessarily tell us which steps actually happened or how to
recover after the writer disappeared.

An idempotency table helps, but it is not enough on its own either. Its command
row can say “processing” after events committed, its stored result can outlive
the state it was derived from, and its completion write can fail after the
domain write succeeded. If that table is treated as domain truth, uncertainty
is hidden rather than resolved.

Rollback Engine explores a stricter answer:

> Preserve committed domain history, assign one authority to each kind of
> decision, reconstruct before trusting derived state, and fail closed when the
> truth cannot be established.

## The idea in five minutes

```text
HTTP request
    │
    ▼
RollbackEngine ─────────────── CommandExecutionCoordinator
    │ domain use cases              │ reservation, idempotency,
    │                               │ commit uncertainty, retries
    │                               ▼
    │                          Command Store
    │                          ownership + generation
    │
    ├── append raw fact ─────────▶ Event Store
    │                                  │
    │                                  │ raw history
    │                                  ▼
    │                               Upcaster
    │                                  │ current representation
    │                                  ▼
    └── authoritative state ◀────── Projection
                                       │
                         ┌─────────────┴─────────────┐
                         ▼                           ▼
                  Materialized View              Snapshot
                    derived cache            replay optimization
```

The important part is not the boxes. It is which box is allowed to decide
what.

## Authority map

| Question | Authority | What must not decide it |
| --- | --- | --- |
| Does an event exist? | Event Store raw history | Materialized view, snapshot, command result |
| In which aggregate order did it happen? | Event Store sequence | Timestamp or read-model version alone |
| What does the history mean now? | Validated upcaster chain and deterministic projection | Cache contents |
| Who may continue a command? | Command Store status, owner and generation | Coordinator-local memory or a worker clock |
| Is a lease challenge valid? | Command Store and its lease clock | Coordinator-owned time |
| What is the authoritative domain state? | Full replay of Event Store history | Materialized view or snapshot |
| Did a lost append acknowledgement hide a commit? | Exact comparison with persisted raw history | Upcast output or identity fields alone |

### Event Store: domain history

The Event Store contains the retained facts. It enforces technical history
invariants such as globally unique event IDs, contiguous aggregate sequences,
non-decreasing aggregate timestamps and expected-version appends.

Events are appended; they are not edited to make the present look tidy. A
rollback is therefore represented by new compensation events, not by deleting
the original events.

### Projection: current domain meaning

The projection folds an ordered stream into current state. It is also where the
supported domain transition rules live: inventory must match the order,
refunds precede inventory release, active resources block deletion, and a
rolled-back order cannot silently reactivate.

Same validated event stream plus the same projection must produce the same
state.

### Materialized views: fast, disposable state

The State Repository stores a convenient read model. It can be stale,
unavailable or corrupt without changing domain history.

Writes use compare-and-swap so one repair cannot blindly overwrite another.
CAS alone is not considered proof of correctness: after contention, a state is
returned to an authoritative caller only when fresh replay validates it. If
that cannot be established within the bounded reconciliation window, the
operation fails closed.

### Snapshots: a shortcut, never truth

A snapshot avoids replaying an entire stream. Before use, it is checked against
the corresponding event prefix. Missing, malformed, ahead or inconsistent
snapshots fall back to full replay.

Deleting every snapshot must affect performance, not meaning.

### Command Store: command ownership

The Command Store owns reservation status, lease ownership and monotonically
increasing generations. That makes it the authority for who may continue a
command, but not for what happened in the domain.

The Coordinator uses it for idempotency and recovery bookkeeping, then checks
command history against the Event Store. A command row cannot erase or invent a
domain event.

### Upcasters: interpretation without a new past

Historical events remain stored in their original representation. Upcasters
translate that representation for current code.

They may migrate payload shape and advance schema versions, but they may not
change historical identity:

- `eventId`
- `aggregateId`
- `sequence`
- `timestamp`
- `eventType`
- `commandId`
- `correlationId`
- `causationId`

Upcaster output is materialized, deeply frozen and validated before projection.
Each migration edge runs twice against independent immutable inputs to catch
common clock-, randomness- and mutable-state dependencies. Missing migration
steps, invalid output, identity mutation or observed non-determinism fail
closed.

## Reliability problems covered

### Unknown commit after a lost acknowledgement

An append can commit and still throw because its acknowledgement was lost. On
that path, the Coordinator asks the Event Store for the persisted **raw** event
and compares it with the original append intent.

This boundary deliberately does not compare an upcast event with a raw event,
and it does not accept matching identity as sufficient proof. Same ID plus a
different raw payload is an inconsistency, not a successful reconciliation.

### Partial multi-step execution

Checkout is a small saga. Successful forward steps become events. Recognized
business failures produce compensating facts in reverse order:

```text
ORDER_CREATED
INVENTORY_RESERVED
PAYMENT_CHARGED
PAYMENT_REFUNDED
INVENTORY_RELEASED
ORDER_ROLLED_BACK
```

Infrastructure and programming failures do not trigger blind compensation.
The engine compensates only when it understands the domain situation.

### Retry and idempotency safety

A stable command key identifies one normalized command. Reusing it with the
same command does not append duplicate events; reusing it for different input
is rejected.

Errors expose whether a commit is known:

| `eventCommitted` | Meaning |
| --- | --- |
| `true` | At least one event is proven committed. Do not execute as a new command. |
| `false` | This failed attempt is proven to have committed no event. |
| `null` | The outcome is still unknown. Reconcile with the same key; do not guess. |

### Expired workers and zombie writes

Leases answer when ownership may be challenged. Generations and fencing tokens
answer whether the worker attempting a mutation still owns the current command
generation.

The clock used for lease expiry belongs to the Command Store. The Coordinator
does not pre-judge expiry with its own clock, so it cannot suppress a legitimate
takeover challenge before the authority is consulted.

### Concurrent writers

Event appends use aggregate `expectedVersion`. Stale writers fail instead of
extending a stream based on an obsolete state assumption.

Materialized-view writers use compare-and-swap, while replay remains the
semantic validator after a race.

### Derived-state corruption

Materialized views and snapshots are useful precisely because they can be
discarded and rebuilt. Command execution, compensation and newly completed
command results use replay-validated state rather than trusting a cache.

## Closed authority boundaries

These labels come from the project’s adversarial architecture reviews:

| Finding | Containment now enforced |
| --- | --- |
| **A-1** | Exhausted materialized-view CAS reconciliation cannot promote an unvalidated view into domain authority. |
| **F-2** | Upcasters cannot mutate immutable event identity or supply unchecked event output. |
| **F-3** | Callback-controlled accessors and proxies lose control after one-time output materialization; validated output is immutable. |
| **F-4** | Normal command execution and completion are validated against authoritative replay rather than derived state. |
| **F-6** | Lost-ACK reconciliation confirms the exact persisted raw event, independently of its upcast representation. |

These are architecture boundaries backed by adversarial tests, not claims that
every possible distributed-systems problem has been solved.

## Guarantees inside the current model

With commands going through `RollbackEngine` and the provided adapters:

- the Event Store remains the retained domain history;
- aggregate order is sequence-based and optimistic concurrency rejects stale
  appends;
- authoritative state is reconstructed by replay;
- invalid supported domain transitions are rejected by projection;
- materialized views and snapshots cannot become domain truth merely because
  they contain a plausible version;
- recovery repairs derived state without generating new domain events;
- current command generation fences stale workers;
- invalid upcasts fail before projection, snapshot creation, view repair or
  normal command reconciliation consumes them;
- a confirmed lost-ACK append cannot create a duplicate event on retry;
- failure responses preserve known, absent and unknown commit outcomes instead
  of collapsing them into one generic error.

## Deliberate limits

This repository is a reliability research project and reference
implementation, not a distributed transaction manager.

### No multi-node consensus guarantee

SQLite mode exercises persistence, multi-process contention and fencing, but
the project does not implement Raft, Paxos, quorum replication or a globally
available consensus service. It does not claim linearizable command ownership
across an arbitrary cluster or survive every network partition.

### No atomic control over external side effects

The engine cannot atomically commit its Event Store together with a payment
provider, warehouse API or any other independent system. If an external API
performs an irreversible action and loses its response, that external reality
must be reconciled through the provider’s own idempotency keys, status API,
receipts or manual resolution.

Internal history can tell us what the engine knows. It cannot magically prove
what happened beyond its trust boundary.

### Upcasters are trusted versioned code

Runtime checks protect identity, structure and several forms of observable
non-determinism. They cannot prove that arbitrary JavaScript is pure or that a
deterministic payload migration preserves the correct business meaning.
Upcasters still require code review, versioned fixtures and controlled rollout.
They must not depend on current configuration, mutable databases or external
APIs.

### Adapters and producers are trust boundaries

The reference stores enforce their documented contracts, but there is no
Byzantine adapter model. A custom adapter that lies about persisted history can
invalidate the guarantees. Likewise, supported command paths use replay and
projection for domain decisions; the Event Store is not a universal semantic
firewall for arbitrary code that bypasses the engine and writes events directly.

## Run it

Use a current Node.js release. SQLite mode requires the built-in `node:sqlite`
module.

```bash
npm install
npm test
npm start
```

The default server runs at `http://localhost:3000` with in-memory storage.

Persistent SQLite mode:

```bash
STORAGE=sqlite DB_PATH=./rollback-engine.db npm start
```

Try a checkout with a stable command identity:

```bash
curl --request POST http://localhost:3000/checkout \
  --header 'content-type: application/json' \
  --header 'Idempotency-Key: checkout-123' \
  --header 'X-Correlation-Id: customer-flow-456' \
  --data '{"item":"Pizza","quantity":1,"amount":100}'
```

Repeat the same request with the same key to receive the stored outcome without
appending the checkout events again. Reusing the key with different input is an
idempotency conflict.

## Useful endpoints

The complete OpenAPI 3.1 contract lives in [`openapi.json`](./openapi.json) and
is also served by the application.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/openapi.json` | Machine-readable API contract |
| `POST` | `/order` | Create an order |
| `GET` | `/order/:id?consistency=materialized` | Fast derived-state read |
| `GET` | `/order/:id?consistency=authoritative` | Replay-validated read |
| `DELETE` | `/order/:id` | Delete an inactive or compensated order |
| `POST` | `/checkout` | Run checkout and supported compensation paths |
| `GET` | `/orders` | List materialized orders |
| `GET` | `/history` | Inspect committed events |
| `GET` | `/diagnostics` | Inspect best-effort reliability diagnostics |
| `GET` | `/timeline/:orderId` | Inspect event and trace relationships |
| `GET` | `/replay-state/:orderId` | Rebuild authoritative state from history |
| `GET` | `/snapshot/:orderId` | Inspect the current snapshot |
| `POST` | `/replay-restore/:orderId` | Repair derived state from retained history |
| `GET` | `/state-at/:orderId/:timestamp` | Replay state at a point in time |
| `GET` | `/state-at/:orderId/sequence/:sequence` | Replay an exact sequence prefix |

## Lab and chaos runner

The repository includes two ways to make failures visible rather than merely
describe them.

Start the guarded browser lab:

```bash
npm run lab
```

The `/lab` UI and scenario mutation endpoints exist only while `LAB_MODE` is
enabled. Normal startup returns `404` for them.

Run deterministic chaos campaigns:

```bash
npm run chaos -- --seed=42 --iterations=100 --profile=memory
```

Campaigns exercise replay equality, compensation ordering, idempotency,
materialized-view drift, restarts and concurrency with reproducible seeds.

## Project layout

```text
src/
  application/      command coordination, replay, recovery, diagnostics
  domain/           events, projection, saga, upcasting
  infrastructure/   in-memory and SQLite store adapters
  chaos/            deterministic invariant campaigns
  lab/              guarded failure scenarios and visual lab
  routes/            HTTP transport layer
tests/               contracts, regressions and adversarial scenarios
```

## Testing philosophy

The test suite is the executable architecture record. Happy-path examples are
only the beginning; the interesting tests deliberately corrupt snapshots and
views, lose append acknowledgements, race CAS writers, expire leases, revive
zombie workers, mutate upcaster identity, simulate restarts and compare replay
with every derived representation.

```bash
npm test       # 528 tests at the current checkpoint
npm run check  # syntax checks for application entry points
git diff --check
```

The goal is not to make failure disappear. It is to make uncertainty explicit,
keep authority in the component that owns the truth, and leave enough history
to recover without guessing.
