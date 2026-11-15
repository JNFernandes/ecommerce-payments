# Data Model: Transactional Outbox (US-07)

The Domain is unchanged: `Payment`, the domain events and `CancellationMarker` stay as they are.

## Table `payment_outbox` (was `payment_dead_letters`)

It is created by migration `TurnDeadLettersIntoOutbox`. The "From" column shows where each
column comes from in the old table.

| Column | Type | Constraints | From |
|---|---|---|---|
| `Id` | `uuid` | PK | unchanged |
| `Sequence` | `bigint` | identity, unique; recording order | **new**; backfilled by `RecordedAt, Id` |
| `EventId` | `uuid` | NOT NULL, unique | **new**; backfilled from `Payload->>'eventId'` |
| `PaymentId` | `uuid` | NOT NULL | unchanged (also the Kafka key) |
| `EventType` | `text` | NOT NULL | unchanged (`PaymentProcessedEvent`, …) |
| `Topic` | `text` | NOT NULL | unchanged |
| `MessageKey` | `text` | NOT NULL | unchanged (payment id) |
| `Payload` | `jsonb` | NOT NULL | unchanged; never modified |
| `Status` | `varchar(20)` | `Pending` / `Published` / `Exhausted` | `PendingRetry`→`Pending`, `Resolved`→`Published` |
| `RetryCount` | `integer` | NOT NULL, default 0 | unchanged |
| `NextAttemptAt` | `timestamptz` | NOT NULL | renamed from `NextRetryAt`; nulls → `CreatedAt` |
| `LastAttemptAt` | `timestamptz` | nullable | unchanged |
| `LastError` | `text` | nullable | unchanged; filled from `FailureReason` where null |
| `RecordedAt` | `timestamptz` | NOT NULL | renamed from `CreatedAt` |
| `PublishedAt` | `timestamptz` | nullable | renamed from `ResolvedAt` |
| ~~`FailureReason`~~ | | | **dropped** after the copy |

Indexes:
- `IX_payment_outbox_due` on (`NextAttemptAt`), partial, where `"Status" = 'Pending'`. It
  replaces `IX_payment_dead_letters_due`.
- `IX_payment_outbox_payment_sequence` on (`PaymentId`, `Sequence`). Used for the per-payment
  ordering check.
- `IX_payment_outbox_event_id`, unique, on (`EventId`).

### Statuses

```
Pending ──publish OK──▶ Published
   │ fail (RetryCount < MaxRetries): RetryCount++, NextAttemptAt = now + backoff
   └─fail (RetryCount reaches MaxRetries)──▶ Exhausted ──operator re-queue──▶ Pending
```

A `Pending` entry is eligible only when every earlier entry (lower `Sequence`) for the same
`PaymentId` is `Published`.

## Service (`Ecommerce.Payments.Service`)

| Type | Kind | Change |
|---|---|---|
| `Payments/IPaymentOutbox` | interface, new | `AddProcessedAsync(PaymentProcessed)`, `AddFailedAsync(PaymentFailed)`, `AddRefundedAsync(PaymentRefunded)`. Its doc says to call it inside the order transaction, before commit. |
| `Payments/IPaymentEventPublisher` | interface | **deleted** |
| `ProcessPaymentHandler`, `RefundPaymentHandler` | changed | Inject `IPaymentOutbox` instead of `IPaymentEventPublisher`. Add the entry inside the transaction, after the save and before `CommitAsync`. Publish nothing after the commit. |

## Infrastructure (`Ecommerce.Payments.Infrastructure`)

| Type | Path | Kind |
|---|---|---|
| `OutboxEntryEntity` | `Persistence/Entities/` | renamed from `PaymentDeadLetterEntity`; columns as above; XML docs |
| `OutboxStatus` | `Persistence/Entities/` | renamed from `DeadLetterStatus`: `Pending`, `Published`, `Exhausted` |
| `IPaymentOutboxRepository` / `PaymentOutboxRepository` | `Persistence/` | renamed. `BeginTransactionAsync`, `ClaimNextDueAsync(now)` (R4 query, one row), `CountBlockedAfterAsync(paymentId, sequence)`, `SaveChangesAsync`. `AddAsync` is removed, because `PaymentOutbox` adds entries. |
| `PaymentOutbox` | `Outbox/` | new; implements `IPaymentOutbox`. Builds and serialises integration events (moved from `PaymentEventPublisher`), then adds a `Pending` entry. |
| `OutboxRelayWorker`, `OutboxRelayProcessor`, `IOutboxRelayProcessor`, `OutboxRetryPolicy`, `OutboxRelayOptions` | `Outbox/` | renamed from `DeadLetters/`. Options: `PollIntervalMs` 250, `BatchSize` 100, `BaseDelayMs` 5000, `MaxDelayMs` 600000, `MaxRetries` 8. |
| `PaymentEventPublisher` | `Messaging/` | reduced to `IRawEventPublisher` (Kafka producer, `MessageTimeoutMs` 10 s); dead-letter code removed |
| `PaymentsDbContext` | `Persistence/` | `PaymentOutbox` set; mapping for the renamed table and its indexes |

## Configuration

`OutboxRelay` section, which replaces `DeadLetterRetry`:

```json
"OutboxRelay": { "PollIntervalMs": 250, "BatchSize": 100, "BaseDelayMs": 5000, "MaxDelayMs": 600000, "MaxRetries": 8 }
```

It is validated at startup, like today.
