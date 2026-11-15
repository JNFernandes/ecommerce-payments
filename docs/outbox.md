# Outbox (outbound payment events)

Every payment event (`PaymentProcessed`, `PaymentFailed`, `PaymentRefunded`) is written to the
`payment_outbox` table **in the same transaction** as the payment change that produced it. The
**outbox relay** then publishes it to Kafka. A recorded outcome is therefore never left
unannounced, even if the process crashes, and events for one payment reach Kafka in the order
they happened.

- Design decision: [ADR 0006, Transactional outbox](adr/0006-transactional-outbox.md). It
  supersedes ADR 0003; the claim and backoff design is unchanged.
- Feature spec: [`specs/007-transactional-outbox`](../specs/007-transactional-outbox/spec.md) (US-07).
- Inbound messages Payments could not handle are a separate table:
  [consumed-dead-letters.md](consumed-dead-letters.md) (US-06).

## How it works

1. A handler records the event through `IPaymentOutbox`. This happens inside the per-order
   transaction, after the payment save and before the commit
   ([ADR 0004](adr/0004-per-order-serialisation.md)). The entry is `Pending` and due at once.
   It already holds the exact payload, `eventId`, topic and key that will be published. The
   handler never publishes.
2. The Kafka offset of the inbound message is committed once that transaction has committed.
3. The `OutboxRelayWorker`, a hosted service in the Consumer host, runs a cycle immediately and
   then every `PollIntervalMs`. A cycle handles up to `BatchSize` entries, **one transaction per
   entry**:
   - **Claim** the next due entry, in recording order (`Sequence`). An entry is eligible only
     when every earlier entry for the **same payment** is `Published`. The claim uses
     `FOR UPDATE SKIP LOCKED`.
   - **Publish** the stored payload **unchanged** to the stored topic, with the stored key.
   - **Update** the entry, then commit.
4. Several instances can run at once. An entry locked by one instance is skipped by the others,
   and its later entries wait, so no entry is published twice concurrently and order is kept.

The relay never blocks startup: it yields before any I/O, and Kafka can be down at boot. When a
cycle fails, it logs the error and continues. It stops cleanly on shutdown. All time decisions
use `TimeProvider`.

## States

```
              publish OK
  Pending ─────────────▶ Published     (PublishedAt set; terminal)
    │  ▲
    │  └─ publish fails, RetryCount < MaxRetries:
    │       RetryCount+1, LastError, LastAttemptAt, NextAttemptAt = now + backoff
    │
    └──── publish fails and RetryCount reaches MaxRetries ──▶ Exhausted (error logged)
                                                                │
                                         operator re-queue ─────┘ (back to Pending)
```

| Status | Meaning | Published by the relay? |
|---|---|---|
| `Pending` | Waiting. Eligible once `NextAttemptAt <= now` **and** every earlier entry for the payment is `Published`. | Yes |
| `Published` | Published successfully (`PublishedAt`). | Never again |
| `Exhausted` | Failed `MaxRetries` times. An error with full context was logged, including how many later events for the payment are waiting. **Blocks later events for the same payment.** | Only after a re-queue |

`EventId`, `Payload`, `Topic`, `MessageKey`, `EventType`, `PaymentId`, `Sequence` and
`RecordedAt` never change.

## Settings

The settings live in section `OutboxRelay` of `src/Ecommerce.Payments.Consumer/appsettings.json`.
Override them with environment variables, for example `OutboxRelay__PollIntervalMs=1000`.
Invalid values stop the service at startup: anything ≤ 0, or `MaxDelayMs < BaseDelayMs`.

| Setting | Default | Meaning |
|---|---|---|
| `PollIntervalMs` | `250` | Time between cycles. Bounds the healthy-path latency (an event is visible under 1 s at p95). |
| `BatchSize` | `100` | Maximum entries per cycle. |
| `BaseDelayMs` | `5000` | Delay after the first failed publish attempt. |
| `MaxDelayMs` | `600000` | Cap on the delay (10 minutes). |
| `MaxRetries` | `8` | Failed attempts before `Exhausted`. |

**Backoff** after the *n*-th failed attempt is `min(BaseDelayMs × 2^(n−1), MaxDelayMs)`. With
the defaults that is 5 s, 10 s, 20 s, 40 s, 80 s, 160 s, 320 s; the 8th failure marks the entry
`Exhausted`, after about 11 minutes. One attempt against an unreachable broker fails within
10 s (`MessageTimeoutMs`).

## Delivery guarantees

- **Never lost.** An outcome and its event are saved together. Either both exist or neither
  does.
- **At-least-once.** A crash after publishing but before marking the entry publishes it again.
  Consumers deduplicate by `eventId`, which **never changes**.
- **In order per payment.** Kafka keeps order within a partition, and the key is the payment id.
  The relay never publishes a later entry before an earlier one. Different payments are
  independent.

## Useful queries

```sql
-- Backlog by status
SELECT "Status", count(*) FROM payment_outbox GROUP BY "Status";

-- Pending entries that have already failed at least once
SELECT "Id", "PaymentId", "EventType", "RetryCount", "NextAttemptAt", "LastError"
FROM payment_outbox WHERE "Status" = 'Pending' AND "RetryCount" > 0 ORDER BY "Sequence";

-- Entries needing attention, with the events waiting behind each one
SELECT x."Id", x."PaymentId", x."EventType", x."RetryCount", x."LastError",
       (SELECT count(*) FROM payment_outbox l
        WHERE l."PaymentId" = x."PaymentId" AND l."Sequence" > x."Sequence" AND l."Status" = 'Pending') AS blocked
FROM payment_outbox x WHERE x."Status" = 'Exhausted' ORDER BY x."LastAttemptAt" DESC;
```

## Re-queue an `Exhausted` entry

Fix the cause first, for example a missing topic or broker configuration. Then:

```sql
UPDATE payment_outbox
SET "Status" = 'Pending', "RetryCount" = 0, "NextAttemptAt" = now()
WHERE "Id" = '<id>';
```

The relay publishes it on its next cycle, followed by the later entries for that payment, in
order.

## Upgrading from the dead-letter table (US-03)

Migration `TurnDeadLettersIntoOutbox` converts `payment_dead_letters` in place:

| Before (US-03) | After (US-07) |
|---|---|
| table `payment_dead_letters` | table `payment_outbox` |
| `PendingRetry` / `Resolved` / `Exhausted` | `Pending` / `Published` / `Exhausted` |
| `CreatedAt`, `NextRetryAt`, `ResolvedAt` | `RecordedAt`, `NextAttemptAt`, `PublishedAt` |
| `FailureReason` | dropped, copied into `LastError` where that was empty |
| — | `EventId` (from the payload), `Sequence` (recording order) |

Rows waiting for a retry become `Pending` and are published by the relay, with no loss and no
duplicate. To upgrade:
1. Stop the service.
2. Run `dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`.
3. Start the service.

Rename the configuration section `DeadLetterRetry` to `OutboxRelay`, and `IntervalMs` to
`PollIntervalMs`.
