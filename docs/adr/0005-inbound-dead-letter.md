# 0005. Inbound dead-letter after bounded consumer retries

- **Status**: Accepted
- **Date**: 2026-11-15

## Context

Since US-04, `KafkaConsumerBase` retries a message whose handling fails **in place**: it seeks
back and waits with capped exponential backoff, and it never commits past the failed message.
That keeps delivery at-least-once, but a message that can never succeed is retried for ever,
and every later message on its partition waits behind it. Payments and refunds for other orders
stop until someone intervenes. Malformed envelopes were the opposite case: they were logged and
committed, so their content survived only in the logs.

Constitution v1.2.0, Principle VI, requires failed message processing to be "retried with
exponential backoff before being routed to a dead-letter table/topic". US-04 recorded the
missing routing as an open item. `ecommerce-inventory` solves the same problem with a
`consumed_dead_letters` table.

## Decision

We will:

1. **Bound the retries.** The consume loop counts failed attempts for the current message,
   tracked by its topic, partition and offset. After `Kafka:ConsumerMaxAttempts` (default 5), it
   stores the message as received in `consumed_dead_letters`, logs an Error, and only then
   commits the offset.
2. **Dead-letter malformed messages at once.** The consumers throw `InvalidMessageException`.
   The base stores the message with `Attempts = 1` and a Warning, without backoff or retries.
3. **Make storing idempotent.** The table has a unique index on (Topic, Partition, Offset), and
   the insert is `ON CONFLICT DO NOTHING`. A crash between storing and committing only causes a
   harmless second insert.
4. **Never commit on a failed store.** If storing fails, typically because the database is down,
   nothing is committed. The message is retried with backoff, and normal handling runs first each
   time, so a recovered database processes the message instead of parking it.
5. **Replay by hand.** Replay is a documented manual procedure
   ([consumed-dead-letters.md](../consumed-dead-letters.md)): produce the stored payload with its
   key to its original topic. Handler idempotency makes this safe.

Rejected alternatives:

- **A dead-letter Kafka topic**: needs topic provisioning in `ecommerce-infra`, which has
  auto-create disabled, and is harder to inspect.
- **Routing through the Service layer**: would teach the Service about Kafka positions. The
  constitution already assigns dead-letter routing to the Consumer.
- **A persisted attempt count**: costs a write per failure to save, at most, some extra retries
  after a restart.
- **Automatic replay**: a message that kept failing needs a human to fix its cause first.

## Consequences

- One bad message can no longer stall a partition for more than about 15 s with the default
  settings, and no message is lost while the database is unavailable.
- A parked message is skipped while later messages on its partition continue, so ordering for
  that order is broken until it is replayed. The saga tolerates this: events are idempotent, and
  US-05 markers handle cancel-before-place.
- An `OrderCancelled` parked for a Processed payment leaves it Processed until the message is
  replayed. Operators must watch for the Error "Dead-lettered message …".
- Rows are never removed by the service. Cleaning them up is a later concern.
- New consumers must throw `InvalidMessageException` for malformed envelopes and let other
  failures propagate to the base.
