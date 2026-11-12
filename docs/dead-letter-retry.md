# Dead-letter retry

When a payment outcome is saved to PostgreSQL but publishing it to Kafka fails, the event is
written to the `payment_dead_letters` table instead of being lost (Constitution Principle II).
The **dead-letter retry worker** republishes those rows automatically, so a temporary broker
outage never loses a `PaymentProcessed` or `PaymentFailed` event.

Design decision: [ADR 0003 — Dead-letter retry](adr/0003-dead-letter-retry.md).
Feature spec: [`specs/003-dead-letter-retry`](../specs/003-dead-letter-retry/spec.md) (US-03).

## How it works

1. The normal publish path (`PaymentEventPublisher`) fails → it inserts a row with the exact
   payload, topic and key, `Status = PendingRetry`, `RetryCount = 0`, `NextRetryAt = now`.
2. Every `IntervalMs` the `DeadLetterRetryWorker` (a hosted service in the Consumer host) runs a
   cycle. For up to `BatchSize` rows, **one transaction per row**:
   - claim one due row: `Status = 'PendingRetry' AND "NextRetryAt" <= now`, oldest first, with
     `FOR UPDATE SKIP LOCKED`;
   - republish the stored payload **unchanged** to the stored topic with the stored key, through
     `IRawEventPublisher.PublishRawAsync` (which never writes a new dead letter);
   - update the row and commit.
3. Several instances can run at once: a row locked by one instance is skipped by the others, so
   no row is processed twice concurrently.

The worker never blocks startup (it yields before any I/O; Kafka can be down at boot), logs and
continues when a cycle fails, and stops cleanly on shutdown. All time decisions use
`TimeProvider`.

## States

```
                publish OK
 PendingRetry ─────────────▶ Resolved     (ResolvedAt set; terminal)
     │  ▲
     │  └─ publish fails, RetryCount < MaxRetries:
     │       RetryCount+1, LastError, LastAttemptAt, NextRetryAt = now + backoff
     │
     └──── publish fails and RetryCount reaches MaxRetries ──▶ Exhausted (terminal, error logged)
```

| Status | Meaning | Retried? |
|---|---|---|
| `PendingRetry` | Waiting for the worker; eligible once `NextRetryAt <= now`. | Yes |
| `Resolved` | Republished successfully (`ResolvedAt`). | Never again |
| `Exhausted` | Failed `MaxRetries` times; an error with full context was logged. Needs operator attention. | Never again |

`Payload`, `Topic`, `MessageKey`, `EventType`, `FailureReason` (the original error) and
`CreatedAt` never change.

## Settings

Section `DeadLetterRetry` in `src/Ecommerce.Payments.Consumer/appsettings.json`; override with
environment variables such as `DeadLetterRetry__IntervalMs=5000`. Invalid values (≤ 0, or
`MaxDelayMs < BaseDelayMs`) stop the service at startup.

| Setting | Default | Meaning |
|---|---|---|
| `IntervalMs` | `30000` | Time between cycles. |
| `BatchSize` | `20` | Maximum rows per cycle. |
| `BaseDelayMs` | `5000` | Delay after the first failed retry. |
| `MaxDelayMs` | `600000` | Cap on the delay (10 min). |
| `MaxRetries` | `8` | Failed retries before `Exhausted`. |

**Backoff** after the *n*-th failed retry: `min(BaseDelayMs × 2^(n−1), MaxDelayMs)`. With the
defaults: 5 s, 10 s, 20 s, 40 s, 80 s, 160 s, 320 s, then the 8th failure marks the row
`Exhausted` — about 11 minutes of retrying in total. The 10-minute cap only applies with larger
`MaxRetries` or `BaseDelayMs`.

## Delivery guarantees

- **At-least-once.** An event can arrive more than once (e.g. the original publish reached the
  broker but timed out, or the row update failed after a successful republish). Downstream
  consumers deduplicate by `eventId`, which **never changes** on retry.
- **No ordering** guarantee between different dead-lettered events.

## Try it locally

With `ecommerce-infra` running and migrations applied
(`dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`),
insert a due test row:

```sql
INSERT INTO payment_dead_letters
    ("Id", "PaymentId", "EventType", "Payload", "Topic", "MessageKey",
     "FailureReason", "CreatedAt", "Status", "RetryCount", "NextRetryAt")
VALUES (
    '3f1c2a9e-0000-4000-8000-000000000001',
    '7b0e8d4c-0000-4000-8000-000000000002',
    'PaymentProcessedEvent',
    '{"eventId":"c5a1f0de-0000-4000-8000-000000000003",
      "occurredAt":"2026-11-12T10:00:00+00:00",
      "aggregateId":"7b0e8d4c-0000-4000-8000-000000000002",
      "version":1,
      "orderId":"9d2b6e11-0000-4000-8000-000000000004",
      "amount":129.99,
      "currency":"USD",
      "processedAt":"2026-11-12T10:00:00+00:00"}',
    'payments.payment-processed',
    '7b0e8d4c-0000-4000-8000-000000000002',
    'Local: Message timed out (test row)',
    now(),
    'PendingRetry',
    0,
    now()
);
```

Run the service with a short interval and watch the row resolve:

```powershell
$env:DeadLetterRetry__IntervalMs = "5000"
dotnet run --project src/Ecommerce.Payments.Consumer
```

## Useful queries

```sql
-- Rows needing attention
SELECT "Id", "PaymentId", "EventType", "Topic", "RetryCount", "LastError", "LastAttemptAt"
FROM payment_dead_letters WHERE "Status" = 'Exhausted' ORDER BY "LastAttemptAt" DESC;

-- Backlog waiting for retry
SELECT "Status", count(*) FROM payment_dead_letters GROUP BY "Status";
```

## Out of scope

No admin API or UI, no manual replay or purge command, no alerting (US-03). Failed **inbound**
messages are not dead-lettered — the consumer leaves their offset uncommitted and Kafka
redelivers them.
