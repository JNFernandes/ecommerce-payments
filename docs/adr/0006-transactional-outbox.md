# 0006. Transactional outbox for payment events

- **Status**: Accepted
- **Date**: 2026-11-15
- **Supersedes**: [0003](0003-dead-letter-retry.md)

## Context

Up to US-06 a payment outcome was saved to PostgreSQL first and then published to Kafka. When
the publish failed, the event was written to `payment_dead_letters` and republished by a
background worker (ADR 0003). Two gaps remained:

1. **A crash could lose an event.** If the process stopped after the save but before the
   publish or the dead-letter insert, the outcome was durable but never announced. Orders would
   then wait forever for a payment result.
2. **Events could arrive out of order.** If `PaymentProcessed` was dead-lettered and the payment
   was refunded quickly, `PaymentRefunded` was published at once and reached Orders before the
   processed event.

Constitution v1.3.0 (Principles II and VI) now requires the following:
- every event is recorded in the same transaction as its state change;
- only a background relay publishes;
- events are published in order per payment, within 2 s when healthy;
- there is a single outbound store.

## Decision

We will:

1. **Record events in the payment transaction.** The Service records each event through
   `IPaymentOutbox` inside the per-order transaction (ADR 0004), after the payment save and
   before the commit. The payment change and its event commit or roll back together. Handlers
   never publish, and `IPaymentEventPublisher` is removed.
2. **Evolve the existing table instead of adding one.** Migration `TurnDeadLettersIntoOutbox`
   renames `payment_dead_letters` to `payment_outbox` in place (user decision Q1 = A):
   - statuses `PendingRetry` / `Resolved` become `Pending` / `Published`;
   - `EventId` is taken from the stored payload;
   - `Sequence` follows recording order.

   Rows that were in flight carry over and get published. History is kept.
3. **Publish with a relay.** The relay, `OutboxRelayWorker` / `OutboxRelayProcessor`, keeps
   ADR 0003's design:
   - one transaction per entry;
   - `FOR UPDATE SKIP LOCKED`;
   - the stored payload, key and `eventId` published unchanged;
   - exponential backoff up to `MaxRetries`, then `Exhausted`.

   It polls every 250 ms (`OutboxRelay:PollIntervalMs`) and drains up to `BatchSize` entries
   per cycle.
4. **Keep per-payment order.** An entry is claimed only when every earlier entry for the same
   payment is `Published`. This is a `NOT EXISTS` check on `Sequence`, which reads committed
   state, so it also holds across relay instances. An `Exhausted` entry therefore blocks later
   events for its payment, and the Error log says how many are waiting. Other payments are
   unaffected.

Rejected alternatives:
- **A new outbox table**, retiring the dead-letter table (Q1 option B): more churn and a data
  copy, for the same result.
- **Keeping both stores** (option C): two outbound mechanisms doing nearly the same job, which
  Constitution VI forbids.
- **PostgreSQL `LISTEN/NOTIFY`, or an in-process "wake up" signal after commit**: lower latency,
  but more moving parts. The 250 ms poll already meets the bound.
- **Publishing right after commit, with the relay as a fallback**: two publish paths, and
  ordering would be harder to guarantee.

## Consequences

- No event is lost by a crash, and none is published before its transaction commits.
- Delivery stays at-least-once. A crash after publishing but before marking an entry publishes
  it again with the same `eventId`, which consumers deduplicate.
- When healthy, events reach Kafka within about 250 ms plus the produce time. When idle, the
  relay runs four indexed queries per second per instance.
- An `Exhausted` entry holds back later events for the same payment until an operator re-queues
  it (`docs/outbox.md`). This is deliberate: order wins over speed for one payment.
- Published entries are kept for support. Cleaning them up is a separate concern.
- **Upgrade**: stop the service, run the migration, then start it. Old code must not run against
  `payment_outbox`.
- The configuration section `DeadLetterRetry` becomes `OutboxRelay`, and `IntervalMs` becomes
  `PollIntervalMs`.
