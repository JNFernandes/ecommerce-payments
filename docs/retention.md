# Data retention

The service purges old housekeeping rows by itself, so its tables don't grow for ever. Payments
and other business data are **never** purged.

- Design decision: [ADR 0007, Data retention](adr/0007-data-retention.md).
- Rule: Constitution v1.4.0, Principle VI "Data retention".
- Feature spec: [`specs/009-data-retention`](../specs/009-data-retention/spec.md) (US-09).

## What is purged, and what never is

| Table | Purged when | Never purged |
|---|---|---|
| `payment_outbox` ([outbox.md](outbox.md)) | `Status = 'Published'` and `PublishedAt` older than `OutboxPublishedDays` | `Pending` and `Exhausted` entries, whatever their age |
| `consumed_dead_letters` ([consumed-dead-letters.md](consumed-dead-letters.md)) | `FailedAt` older than `ConsumedDeadLettersDays` | — |
| `order_cancellation_markers` | `RecordedAt` older than `CancellationMarkersDays` | — |
| `payments` | never | everything |

A purge can't block or reorder outbox events. The relay holds an entry back only while an
earlier entry for the same payment *exists* and is not `Published`. A deleted published entry
no longer exists, so nothing waits on it.

## Settings

The settings are in the `Retention` section of `src/Ecommerce.Payments.Consumer/appsettings.json`.
You can override them with environment variables, for example `Retention__OutboxPublishedDays=7`.
Invalid values stop the service at startup. That means an interval or batch size of 0 or less,
or a negative number of days.

| Setting | Default | Meaning |
|---|---|---|
| `IntervalMs` | `3600000` | Time between purge runs (1 hour). The first run happens shortly after startup. |
| `BatchSize` | `1000` | Maximum rows deleted per statement. A run repeats batches until the table has nothing left to purge. |
| `OutboxPublishedDays` | `30` | Days a published outbox entry is kept after it was published. `0` keeps entries forever. |
| `ConsumedDeadLettersDays` | `90` | Days an inbound dead letter is kept. `0` keeps them forever. |
| `CancellationMarkersDays` | `180` | Days a cancellation marker is kept. `0` keeps them forever. See the warning below. |

> **Cancellation markers.** A marker protects a cancelled order from being charged when its
> `OrderPlaced` arrives after the `OrderCancelled`. If the marker is purged first, that late
> `OrderPlaced` **is charged**. Keep `CancellationMarkersDays` far above the longest delay you
> can expect between the two events. The 180-day default is very generous.

> **Dead letters.** Their payload is lost when they are purged. Replay or export the rows you
> still need before `ConsumedDeadLettersDays` passes ([consumed-dead-letters.md](consumed-dead-letters.md)).

## How it runs

- `RetentionWorker` is a hosted service in the Consumer host. It yields before doing anything,
  so startup never waits on the database.
- It runs `RetentionPurger` in a fresh scope.
- Each table is purged in batches. Each batch is one auto-committed statement:
  `DELETE … WHERE key IN (SELECT key … ORDER BY time LIMIT BatchSize FOR UPDATE SKIP LOCKED)`.
  - Locks stay short.
  - Several instances can purge at the same time without waiting on each other or deleting
    the same row twice.
- Each run logs one line. It is logged at Information when anything was deleted, and at Debug
  otherwise:

  ```text
  Retention purge deleted 120 outbox entries, 2 dead letters, 0 cancellation markers.
  ```

- When a run fails, it logs `Retention purge failed; retrying on the next interval.` and the
  next run tries again. Payment processing is not affected.

## What would the next run delete?

```sql
-- Defaults: 30 / 90 / 180 days
SELECT 'outbox' AS store, count(*) FROM payment_outbox
WHERE "Status" = 'Published' AND "PublishedAt" < now() - interval '30 days'
UNION ALL
SELECT 'dead letters', count(*) FROM consumed_dead_letters WHERE "FailedAt" < now() - interval '90 days'
UNION ALL
SELECT 'markers', count(*) FROM order_cancellation_markers WHERE "RecordedAt" < now() - interval '180 days';
```
