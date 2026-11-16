# 0007. Data retention by age with batched, SKIP LOCKED deletes

- **Status**: Accepted
- **Date**: 2026-11-16

## Context

Three tables grow without bound:

- `payment_outbox`: one row per payment event (US-07), kept after publishing;
- `consumed_dead_letters`: one row per inbound message that was malformed or kept failing (US-06);
- `order_cancellation_markers`: one row per cancellation that arrived before its payment (US-05).

None of these rows is needed for ever. Constitution v1.4.0, Principle VI "Data retention", allows
purging them by age under a configurable policy, with three conditions:

- `Pending` and `Exhausted` outbox entries are never purged;
- payments and other business data are never purged;
- the purge must be batched, safe across instances, and never block processing.

## Decision

We will:

1. **Purge by age, with one setting per table.** Settings live in the `Retention` section; `0`
   keeps that table forever.

   | Table | What is purged | Default |
   |---|---|---|
   | `payment_outbox` | `Published` entries whose `PublishedAt` is older than the setting | 30 days |
   | `consumed_dead_letters` | rows whose `FailedAt` is older than the setting | 90 days |
   | `order_cancellation_markers` | rows whose `RecordedAt` is older than the setting | 180 days |

2. **Run a background job.** A `RetentionWorker` runs `RetentionPurger` shortly after startup
   and then every `IntervalMs` (1 hour). It yields before doing anything, logs how many rows it
   deleted per table, and when a run fails it logs the error and tries again on the next run.
3. **Delete in batches with `SKIP LOCKED`.** Each batch is one auto-committed statement:
   `DELETE … WHERE key IN (SELECT key … ORDER BY time LIMIT BatchSize FOR UPDATE SKIP LOCKED)`.
   The purger repeats it until a batch deletes fewer than `BatchSize` (1 000) rows.
   - Each statement is short and holds few locks.
   - Several instances never wait on each other or delete the same row.
   - Rows locked by the outbox relay are skipped, not waited on.
4. **Add age indexes.** Migration `AddRetentionIndexes` adds:
   - `IX_payment_outbox_published_at`, partial on `Status = 'Published'`;
   - `IX_consumed_dead_letters_failed_at`;
   - `IX_order_cancellation_markers_recorded_at`.

**Purging keeps the outbox order intact.** The relay holds an entry back only while an earlier
entry for the same payment exists and is not `Published` (ADR 0006). A deleted published entry
no longer exists, so it cannot block or reorder later events.

**Rejected alternatives:**

- **`pg_cron` or another database-side job**: it needs an extension in `ecommerce-infra` and
  moves the logic out of the service.
- **One unbounded `DELETE`**: long locks and write-ahead-log spikes on a large backlog.
- **Purging inside the outbox relay**: it mixes concerns, and the relay runs every 250 ms.
- **Archiving rows before deleting them**: no consumer needs the archive yet. This can be added
  later.

## Consequences

- The three tables stay bounded. Rows that still matter are never deleted: pending or exhausted
  events, recent dead letters and markers, and payments.
- **Marker trade-off.** A marker purged before its `OrderPlaced` finally arrives lets that late
  `OrderPlaced` charge a cancelled order. The 180-day default is far above any realistic delay.
  Lower it only if Orders can guarantee a tighter bound.
- A dead letter's payload is gone after 90 days. Replay or export it before then.
- The first run on an old database may delete many rows. Because every batch commits on its own,
  that run can be interrupted safely and resumes on the next one.
