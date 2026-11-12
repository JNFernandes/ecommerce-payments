# Data Model: Dead-Letter Retry (US-03)

The `Payment` aggregate and the `payments` table are **unchanged**. Only `payment_dead_letters`
changes, through one EF Core migration (`AddDeadLetterRetryBookkeeping`).

## Entity: `PaymentDeadLetterEntity` → table `payment_dead_letters`

Column names follow the existing EF default (PascalCase, quoted in SQL).

| Column | Type | Null | Existing? | Meaning |
|---|---|---|---|---|
| `Id` | uuid | no | yes | Primary key. |
| `PaymentId` | uuid | no | yes | Payment the event belongs to. |
| `EventType` | text | no | yes | `PaymentProcessedEvent` or `PaymentFailedEvent`. |
| `Payload` | jsonb | no | yes | Exact serialized integration event, including its `eventId`. Never modified. |
| `FailureReason` | text | no | yes | Error of the **original** failed publish. Never modified. |
| `CreatedAt` | timestamptz | no | yes | When the row was created. |
| `Topic` | text | no | **new** | Destination topic of the original publish. |
| `MessageKey` | text | no | **new** | Kafka key of the original publish (payment id). |
| `Status` | varchar(20) | no | **new** | `PendingRetry` \| `Resolved` \| `Exhausted` (enum stored as string). |
| `RetryCount` | integer | no | **new** | Retry attempts made by the worker (default 0). |
| `NextRetryAt` | timestamptz | yes | **new** | When the row is next due; set while `PendingRetry`. |
| `LastAttemptAt` | timestamptz | yes | **new** | Time of the most recent worker attempt. |
| `LastError` | text | yes | **new** | Error of the most recent failed worker attempt. |
| `ResolvedAt` | timestamptz | yes | **new** | When a worker republish succeeded. |

**Index**: `IX_payment_dead_letters_due` on `("NextRetryAt")` `WHERE "Status" = 'PendingRetry'`.

### Migration backfill (existing rows)

- `Status = 'PendingRetry'`, `RetryCount = 0`, `NextRetryAt = now()` (migration time — due
  immediately, FR-002).
- `MessageKey = "PaymentId"::text`.
- `Topic` from `EventType`: `PaymentProcessedEvent` → `payments.payment-processed`,
  `PaymentFailedEvent` → `payments.payment-failed`, otherwise `''` (fails on publish and ends
  `Exhausted`).
- New non-null columns are added with defaults for the backfill, then the SQL above sets the real
  values. `Down` drops the new columns and index.

### New-row values (normal publish path)

`Status = PendingRetry`, `RetryCount = 0`, `NextRetryAt = CreatedAt = TimeProvider now`,
`Topic`/`MessageKey` = those used by the failed publish, other new columns null.

## Enum: `DeadLetterStatus`

`PendingRetry`, `Resolved`, `Exhausted` (in `Infrastructure/Persistence/Entities`).

## State transitions

```
                 publish OK                 
  PendingRetry ─────────────▶ Resolved      (ResolvedAt = now, LastAttemptAt = now)
       │  ▲
       │  │ publish fails, RetryCount+1 < MaxRetries
       │  └── RetryCount += 1, LastError, LastAttemptAt = now,
       │      NextRetryAt = now + min(Base × 2^(RetryCount−1), MaxDelay)
       │
       │ publish fails, RetryCount+1 >= MaxRetries
       └──────────────▶ Exhausted   (RetryCount += 1, LastError, LastAttemptAt = now; error logged)
```

- `Resolved` and `Exhausted` are terminal: never selected by the claim query.
- `Payload`, `EventType`, `Topic`, `MessageKey`, `FailureReason`, `CreatedAt` never change after
  creation, so the republished event is byte-identical and keeps its `eventId` (FR-005).

## Validation rules

- Only rows with `Status = PendingRetry` **and** `NextRetryAt <= now` are eligible (`now` from
  `TimeProvider`).
- A row is processed by at most one transaction at a time (`FOR UPDATE SKIP LOCKED`).
- `RetryCount` never exceeds `MaxRetries`.

## Configuration: `DeadLetterRetryOptions` (section `DeadLetterRetry`)

| Setting | Default | Rule |
|---|---|---|
| `IntervalMs` | 30000 | > 0 |
| `BatchSize` | 20 | > 0 |
| `BaseDelayMs` | 5000 | > 0 |
| `MaxDelayMs` | 600000 | ≥ `BaseDelayMs` |
| `MaxRetries` | 8 | > 0 |
