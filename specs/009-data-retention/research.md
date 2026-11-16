# Research: Data Retention (US-09)

All decisions below are resolved; no NEEDS CLARIFICATION remains.

## R1. Shape: one worker, one purger, three stores

**Decision**: Add a `RetentionWorker` hosted service in the Consumer host, as for the outbox
relay. On each run it calls `IRetentionPurger.PurgeAsync` from a fresh DI scope. The purger
handles the three stores one after another, and returns a `PurgeResult` with a count for each
store. The new code lives in a new `Infrastructure/Retention/` folder.

**Rationale**: This matches the established relay pattern (US-03/US-07), which keeps a small
worker, a testable processor, and options validated at startup. Purging is infrastructure
housekeeping, not business logic, so the Service and Domain layers are not involved (the same
reasoning as the US-06 dead-letter store).

**Alternatives considered**:
- *A PostgreSQL job such as `pg_cron`*: it needs an extension in `ecommerce-infra` and moves the
  logic outside the service.
- *A database TTL*: PostgreSQL has none.
- *Purging inside the relay*: it mixes concerns, and the relay runs every 250 ms.

## R2. Batched, instance-safe deletes

**Decision**: Each batch is a single auto-committed statement:

```sql
DELETE FROM <table>
WHERE "<key>" IN (
  SELECT "<key>" FROM <table>
  WHERE <expired-condition>
  ORDER BY "<time column>"
  LIMIT @batch
  FOR UPDATE SKIP LOCKED)
```

Each store repeats this statement until a batch deletes fewer rows than `BatchSize`.

**Rationale**:
- `LIMIT` bounds how much each statement deletes and how long it holds locks (FR-005).
- `SKIP LOCKED` means two instances never wait on each other. When two purgers run at the same
  time, they simply take different rows, and a row is deleted at most once (FR-006, SC-003).
- Rows the relay is holding (`Pending` rows, which are never expired anyway) are skipped instead
  of waited on.

**Alternatives considered**:
- *One unbounded `DELETE`*: it would cause long locks and WAL spikes on a large backlog.
- *`ctid`-based batching*: it works, but key-based batching is clearer and portable.

## R3. Expired conditions

| Store | Key | Expired when |
|---|---|---|
| `payment_outbox` | `Id` | `"Status" = 'Published' AND "PublishedAt" < cutoff` |
| `consumed_dead_letters` | `Id` | `"FailedAt" < cutoff` |
| `order_cancellation_markers` | `OrderId` | `"RecordedAt" < cutoff` |

- The cutoff is `TimeProvider.GetUtcNow() − days`.
- A store whose retention is `0` is skipped entirely (FR-004).
- `Pending` and `Exhausted` outbox entries never match (FR-001).
- **Ordering is unaffected.** The relay's claim (US-07, R4) blocks on an earlier entry only when
  `Status <> 'Published'`. A deleted published entry no longer exists, so it blocks nothing.
  Purging therefore cannot block or reorder later events. An integration test proves this.

## R4. Indexes

**Decision**: Add migration `AddRetentionIndexes` with three indexes:
- `IX_payment_outbox_published_at`: a partial index on `PublishedAt`, filtered on
  `"Status" = 'Published'`;
- `IX_consumed_dead_letters_failed_at` on `FailedAt`;
- `IX_order_cancellation_markers_recorded_at` on `RecordedAt`.

**Rationale**: the purge selects rows by age. Without these indexes, each hourly batch would
scan the whole table, and the outbox is the fastest-growing table.

## R5. Settings

**Decision**: a `Retention` section, validated at startup (FR-004).

| Setting | Default | Rule |
|---|---|---|
| `IntervalMs` | 3 600 000 (1 hour) | > 0 |
| `BatchSize` | 1 000 | > 0 |
| `OutboxPublishedDays` | 30 | ≥ 0; 0 means keep forever |
| `ConsumedDeadLettersDays` | 90 | ≥ 0 |
| `CancellationMarkersDays` | 180 | ≥ 0 |

Retention is configured in whole days, which is coarse enough for this purpose and simple to
configure.

## R6. Worker behaviour

- `Task.Yield` before any work, so startup never waits on the database (Constitution VI).
- A first run shortly after startup, then every `IntervalMs`, driven by a `PeriodicTimer` on
  the `TimeProvider`.
- Each run uses a fresh scope.
- When a run fails, the worker logs an Error and tries again on the next tick (FR-007).
  Cancellation stops it cleanly.
- After each run it logs a single line:
  "Retention purge deleted {Outbox} outbox entries, {DeadLetters} dead letters, {Markers}
  cancellation markers". This is logged at Information when the total is greater than 0, and at
  Debug otherwise (FR-008).

## R7. Tests

**Unit tests** (`Infrastructure.Tests/Retention/`):
- `RetentionOptions` validation;
- `RetentionWorker` with a fake purger and a `FakeTimeProvider`:
  - a first run, then one run per interval;
  - a failure is logged and the next tick still runs;
  - the log level follows the counts.

**Integration tests** (`Integration.Tests/RetentionPurgerTests.cs`, real PostgreSQL):
- only expired rows of each store are deleted;
- `Pending` and `Exhausted` entries are kept;
- a store set to 0 is untouched;
- a backlog larger than 10 × `BatchSize` is fully purged in one call;
- two purgers running concurrently delete everything expired with no errors, and their totals
  sum to the expected number;
- after purging a published earlier entry, the later `Pending` entry for the same payment is
  still claimable;
- the migration creates the three indexes.

**Component tests**: not needed. There is no Kafka involvement, and the hosting behaviour is
covered by the worker unit tests.

## R8. Documentation

- **README**: add the background-services row and the configuration rows, and remove the
  "never cleaned up" limitations.
- **`docs/outbox.md`** and **`docs/consumed-dead-letters.md`**: add a retention section.
- **New `docs/retention.md`**: settings, what is never purged, the marker trade-off, and queries.
- **ADR 0007**, "Data retention by age with batched, SKIP LOCKED deletes".
- **CLAUDE.md**: the retention note becomes the actual rule.
