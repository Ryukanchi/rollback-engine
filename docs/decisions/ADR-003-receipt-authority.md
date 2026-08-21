# ADR-003: Historically Anchored Command Receipts

- Status: Accepted
- Date: 2026-08-21
- Scope: Completed idempotency reads

## Context

The Command Store owns command reservation, lease, fencing generation, and
execution history. A stored `result`, however, is not Domain History. Returning
it solely because its command record says `completed` would let Command Store
data authorize a domain state that might contradict the Event Store.

A completed result therefore needs a historical proof. It must remain stable
when the aggregate later advances, and its validation must not reintroduce
authority through a materialized view, a snapshot, or the current aggregate
head.

## Decision

A completed command record is a historically anchored receipt. The receipt is
a claim made by the Command Store; Event History and replay prove that claim
before an idempotency read may return it.

The receipt contains:

```text
receiptMetadata
├── contractVersion
├── domainEffect: events | none
└── stateAnchor
    ├── aggregateId
    ├── sequence
    └── lastEventId
```

`eventRange` describes the events produced by this command. `stateAnchor`
describes the historical aggregate state represented by the result. They are
related but not interchangeable: an eventless successful command can have no
effect range while still anchoring a historical state.

Legacy completed records without `receiptMetadata` remain unanchored. The read
path does not infer an anchor from `eventRange` or from current state.

## Validation boundary

The `CommandExecutionCoordinator` orchestrates validation immediately before
returning a completed result:

```text
Command Store completed record
        ↓
receipt envelope and contract validation
        ↓
effect range matched against command Event History
        ↓
state anchor identity matched against aggregate Event History
        ↓
Event Store → upcasting → projection to anchor sequence
        ↓
result.state and optional result.snapshot comparison
        ↓
historical result may be returned
```

The Rollback Engine owns historical projection. The Coordinator does not
calculate Domain State. The comparison stops at `stateAnchor.sequence`; later
events and the current aggregate head are irrelevant.

Validation never reads the materialized state repository or Snapshot Store.
The optional snapshot embedded in the result is only another claim to compare
with replay. It is never an input to reconstruction.

Validation is read-only. A failure does not repair the receipt, mutate the
Command Store, append events, or re-execute the command.

## Failure semantics

| Code | Meaning | Retry contract |
| --- | --- | --- |
| `COMMAND_RECEIPT_UNANCHORED` | A legacy completed record has no historical anchor. | Fail closed; manual resolution. |
| `COMMAND_RECEIPT_INVALID` | The receipt is malformed or contradicts Event History or historical replay. | Fail closed; manual resolution. |
| `COMMAND_RECEIPT_VALIDATION_UNAVAILABLE` | Event History or historical replay could not currently be evaluated. | Retry only the same idempotency key; the completed path never re-executes the command. |

All three outcomes return no result and leave Domain History and command data
unchanged.

## Relationship to raw commit reconciliation

Lost-ACK reconciliation and completed-receipt validation are separate
boundaries:

- Raw commit confirmation determines whether the exact event intended by an
  append is already persisted. It compares the persisted raw representation so
  upcasting cannot create a false mismatch.
- Receipt validation determines whether a stored completed result describes the
  historical Domain State at its anchor. It uses the normal upcast-and-project
  replay path.

Neither boundary can replace the other.

## Consequences

Benefits:

- The Command Store does not become Domain Authority.
- Idempotency reads cannot return manipulated derived state.
- Completed receipts survive later aggregate events.
- Validation failures cannot duplicate command events or external execution.

Costs and limits:

- Completed idempotency reads pay for historical replay.
- Upcasters and projections remain trusted, versioned code and must preserve
  deterministic historical interpretation.
- Legacy unanchored receipts require manual migration or resolution; the engine
  deliberately does not guess.
- The receipt proves internal historical state. It does not make external side
  effects atomic with Event Store commits.

## Rejected alternatives

- Trusting `record.result`: gives Command Store data Domain Authority.
- Comparing with current aggregate state: invalidates legitimate historical
  receipts after later commands.
- Using materialized views or Snapshot Store data: promotes derived state to an
  authority source.
- Inferring missing legacy anchors: manufactures historical proof at read time.
- Re-executing on validation failure: risks duplicate events and external side
  effects.
- Adding technical completion events: changes the event model without being
  necessary for receipt proof.
