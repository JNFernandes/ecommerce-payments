# Data Model: Data Retention (US-09)

There are no new tables or columns. Migration `AddRetentionIndexes` adds three indexes only:

| Index | Table | Columns | Filter |
|---|---|---|---|
| `IX_payment_outbox_published_at` | `payment_outbox` | `PublishedAt` | `"Status" = 'Published'` |
| `IX_consumed_dead_letters_failed_at` | `consumed_dead_letters` | `FailedAt` | — |
| `IX_order_cancellation_markers_recorded_at` | `order_cancellation_markers` | `RecordedAt` | — |

## What is purged

| Store | Expired when | Never purged |
|---|---|---|
| `payment_outbox` | `Status = Published` and `PublishedAt < now − OutboxPublishedDays` | `Pending`, `Exhausted` (any age) |
| `consumed_dead_letters` | `FailedAt < now − ConsumedDeadLettersDays` | — |
| `order_cancellation_markers` | `RecordedAt < now − CancellationMarkersDays` | — |
| `payments` and the other business data | never | everything |

A retention value of `0` switches purging off for that store.

## Types (`Ecommerce.Payments.Infrastructure/Retention/`, new)

| Type | Kind | Notes |
|---|---|---|
| `RetentionOptions` | options class | Section `Retention`: `IntervalMs` (3 600 000), `BatchSize` (1 000), `OutboxPublishedDays` (30), `ConsumedDeadLettersDays` (90), `CancellationMarkersDays` (180). `IsValid()`: interval and batch > 0, days ≥ 0. |
| `PurgeResult` | record | `(int OutboxEntries, int DeadLetters, int Markers)` plus `Total`. |
| `IRetentionPurger` | interface | `Task<PurgeResult> PurgeAsync(CancellationToken)`. |
| `RetentionPurger` | class | Takes `PaymentsDbContext`, `IOptions<RetentionOptions>` and `TimeProvider`. For each enabled store it runs batched `DELETE … WHERE key IN (SELECT … LIMIT n FOR UPDATE SKIP LOCKED)` until a batch deletes fewer than `BatchSize` rows. |
| `RetentionWorker` | `BackgroundService` | `Task.Yield`, then a first run and one run per `IntervalMs` on a `PeriodicTimer(TimeProvider)`. Each run uses a fresh scope. It logs one line per run (Information if anything was deleted, Debug otherwise) and logs an Error on failure, then continues. |
| `RetentionServiceCollectionExtensions` | DI | `AddRetention()` registers `IRetentionPurger` as scoped. The host binds and validates the options, and registers the worker. |

## Configuration (`appsettings.json`)

```json
"Retention": {
  "IntervalMs": 3600000,
  "BatchSize": 1000,
  "OutboxPublishedDays": 30,
  "ConsumedDeadLettersDays": 90,
  "CancellationMarkersDays": 180
}
```
