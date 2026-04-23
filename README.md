# 🧠 Rollback Engine — Event Sourcing Playground

A small but powerful backend system demonstrating **rollback handling, event sourcing, time travel, and state reconstruction**.

This is not a CRUD app.  
This is a **stateful system built around history**.

---

## 🚀 What this project demonstrates

- 🔁 **Saga Pattern (Rollback / Compensation)**
- 🧾 **Event Sourcing (history as source of truth)**
- ⏱️ **Time Travel Debugging**
- 📸 **Snapshots for performance optimization**
- ♻️ **State reconstruction from events**

---

## 🧠 Core Idea

Instead of storing the current state, this system stores **everything that happened**.

> State = function(events)

This allows:
- reconstructing past states
- debugging system behavior
- recovering after crashes

---

## 🧱 Architecture Overview

### 1. Event Store

All actions are stored in memory:

```js
history = []
```

Each event contains:
- type (CREATE_ORDER, RESERVE_INVENTORY, CHARGE_PAYMENT)
- entity snapshot
- timestamp

---

### 2. Checkout Flow (Saga Simulation)

```text
CREATE_ORDER → RESERVE_INVENTORY → CHARGE_PAYMENT → 💥 FAIL
```

Then:

```text
ROLLBACK → refund → release → mark order rolled_back
```

---

### 3. Rollback Engine

Rollback is:
- **state-aware**
- **idempotent**
- **non-destructive**

Entities are not deleted — they are transitioned:
- `charged → refunded`
- `reserved → released`
- `created → rolled_back`

---

### 4. Time Travel

Reconstruct system state at any point in time:

```
GET /state-at/:orderId/:timestamp
```

Example:

```
/state-at/1/2026-04-23T06:54:09.000Z
```

---

### 5. Snapshots

Snapshots store system state at a point in time to optimize replay:

```js
snapshots = Map()
```

Used to:
- reduce replay cost
- speed up reconstruction

---

## 🧪 How to run

```bash
npm install
node app.js
```

Server:

```
http://localhost:3000
```

---

## 🧪 Example Flow

### 1. Trigger a checkout

```
POST /checkout
```

Body:

```json
{
  "item": "Pizza",
  "quantity": 1,
  "amount": 100
}
```

---

### 2. Inspect history

```
GET /history
```

---

### 3. Time travel

```
GET /state-at/1/<timestamp>
```

---

### 4. Replay state

```
GET /replay-state/1
```

---

## ⚠️ Important Notes

- Data is stored **in-memory**
- Restarting the server resets all state
- This is intentional for learning event sourcing concepts

---

## 💡 Why this project matters

Most beginner projects store state directly.

This project explores a different idea:

> Systems can be built around **history instead of state**

This enables:
- reproducibility
- auditability
- deterministic debugging

---

## 🚧 Possible Extensions

- persistent event store (database)
- event versioning
- multi-entity replay
- UI visualization (timeline)
- distributed system simulation

---

## 🧠 One-line takeaway

> I build systems where **state emerges from history — not the other way around.**
