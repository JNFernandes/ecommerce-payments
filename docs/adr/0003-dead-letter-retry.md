# 0003. Dead-letter retry worker with SKIP LOCKED claiming and exponential backoff

- **Status**: Accepted
- **Date**: 2026-11-12

## Context

The write flow saves the `Payment` to PostgreSQL before publishing to Kafka. When the publish
fails after a successful save, the event is stored in the `payment_dead_letters` table and the
consumer offset is still committed (Constitution Principle II). Nothing republishes those rows
yet, so a transient broker outage leaves Orders without the payment outcome. The service may run
as several instances, so a retry mechanism must never publish the same row concurrently from two
instances.

## Decision

We will add a background worker (hosted service) that retries dead-lettered events:

- Each dead-letter row carries a `status` (`PendingRetry`, `Resolved`, `Exhausted`), a retry
  count, a next retry time and the last error, alongside the event type and serialized payload.
- The worker periodically selects due rows (`PendingRetry` with next retry time <= now) and
  claims them with `SELECT ... FOR UPDATE SKIP LOCKED`, so several instances never process the
  same row.
- On success the row becomes `Resolved`. On failure the retry count is incremented, the last
  error is recorded and the next retry time is pushed out with exponential backoff; when the
  configured maximum retry count is reached the row becomes `Exhausted`.
- A retry republishes the stored payload unchanged, keeping the original `eventId`, so
  downstream consumers deduplicate it like any redelivery.

## Consequences

- Transient Kafka failures heal automatically without reprocessing the inbound order event.
- Horizontal scaling is safe without leader election or an external lock service; it relies on
  PostgreSQL row locking.
- Delivery remains at-least-once: a publish that succeeds but whose status update fails will be
  retried, which downstream deduplication by `eventId` absorbs.
- `Exhausted` rows need operational attention (alerting or manual replay); that tooling is out
  of scope here.
- Requires a migration adding the new columns to `payment_dead_letters`, plus configuration for
  poll interval, backoff and maximum retries (US-03).
