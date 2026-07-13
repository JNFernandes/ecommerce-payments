# Data Model: Handle Payment Failure

This extends the `Payment` aggregate and persistence model introduced in
[001-process-payment/data-model.md](../001-process-payment/data-model.md). Only the additions
and changed sections are documented here; everything else from that document still applies
unchanged.

## Domain

### Payment (Aggregate Root) — additions

| Field | Type | Notes |
|---|---|---|
| `FailureReason` | string? | **New.** Populated only when `Status = Failed`. Free-text business reason (see spec Assumptions — no fixed taxonomy required). Null while `Pending`/`Processed`. |
| `FailedAt` | DateTimeOffset? | **New.** When the transition to `Failed` completed. Null unless `Status = Failed`. |

**New entry points**:

- `Payment.Fail(string reason)` — transitions `Pending → Failed`, sets `FailedAt` and
  `FailureReason`, raises `PaymentFailed`. Mirrors `Process()` exactly, including throwing
  `InvalidPaymentTransitionException` (no state change) if not currently `Pending`.
- `Payment.Evaluate(decimal maxAmountThreshold)` — the new orchestration entry point
  `ProcessPaymentHandler` calls instead of bare `Process()`. Compares `Amount` to
  `maxAmountThreshold`; if `Amount` exceeds it, calls `Fail(reason)` with a reason describing the
  threshold breach, otherwise calls `Process()`. Returns whichever `PaymentDomainEvent` resulted.
  This is the *only* place the amount-vs-threshold business rule is evaluated (Constitution
  Principle I).

**Invariants** (additions):

- `FailureReason` MUST be non-empty whenever `Status = Failed`, and MUST be null otherwise —
  enforced by `Fail()` requiring a non-empty `reason` argument and being the only path to
  `Status = Failed`.
- `Status` now also moves `Pending → Failed` (in addition to the existing `Pending → Processed`);
  both are terminal — `InvalidPaymentTransitionException` on any further transition attempt from
  either.

**State transitions** (updated):

```text
Create() --> Pending --Evaluate()--> [internally calls Process() or Fail(reason)]
                |                          |                    |
                |                    Processed              Failed
                |
                +--Process()/Fail() on already-terminal--> throws InvalidPaymentTransitionException (no state change)
```

### PaymentStatus (enum)

No change — `Failed` already exists on this enum (added defensively during US-01, now actually
reachable for the first time).

### PaymentFailed (Domain Event) — new

Raised in-process by `Payment.Fail()` on a successful transition. Mirrors `PaymentProcessed`'s
shape.

| Field | Type |
|---|---|
| `PaymentId` | Guid |
| `OrderId` | Guid |
| `Reason` | string |
| `FailedAt` | DateTimeOffset |

## Integration Events (Kafka payloads)

See [contracts/](./contracts/) for the full wire-format definition of the new event.
`OrderPlacedEvent` (consumed) is unchanged from US-01.

### PaymentFailedEvent (published, `payments.payment-failed`) — new

| Field | Type | Required |
|---|---|---|
| `eventId` | UUID | yes — newly generated for this outbound event |
| `occurredAt` | ISO timestamp | yes |
| `aggregateId` | UUID (payment id) | yes |
| `version` | int | yes |
| `orderId` | UUID | yes |
| `reason` | string | yes |
| `failedAt` | ISO timestamp | yes |

## Persistence (Infrastructure)

### `payments` table — additions

Two new nullable columns, added via a new migration (`AddPaymentFailureFields`):

| Column | Type | Notes |
|---|---|---|
| `FailureReason` | text, nullable | Mirrors `Payment.FailureReason`. |
| `FailedAt` | timestamp with time zone, nullable | Mirrors `Payment.FailedAt`. |

No change to the existing `UNIQUE (order_id)` index — the idempotency guarantee already covers
both outcomes, since it only cares whether *any* row exists for the order, not the row's status.

### `payment_dead_letters` table

No structural change. `PublishFailedAsync`'s dead-letter fallback writes rows here exactly like
`PublishAsync` does today, with `EventType = nameof(PaymentFailedEvent)`.
