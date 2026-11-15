# Contract: Consumed Dead Letters (US-06)

US-06 adds no Kafka topics and no payload changes. The inbound contracts are unchanged:
- [`orders.order-placed`](../../001-process-payment/contracts/orders.order-placed.md)
- [`orders.order-cancelled`](../../004-refund-on-order-cancelled/contracts/orders.order-cancelled.md)

The outbound contracts are unchanged as well.

The new, operator-facing contract is the `consumed_dead_letters` table, described in
[data-model.md](../data-model.md).

## Guarantees

- One row per Kafka position (`Topic`, `Partition`, `Offset`), at most.
- `Payload` and `MessageKey` are exactly what the broker delivered, with no parsing or
  normalisation.
- `Attempts` is 1 for a malformed message. Otherwise it is the number of failed handling
  attempts in the process that stored it: at least `Kafka:ConsumerMaxAttempts` (default 5), and
  more if earlier store attempts failed.
- A message's offset is committed only **after** its row is stored. If the store fails, the
  offset is not committed and the message is retried.
- Rows are never changed or deleted by the service.

## Behaviour seen by Orders

When an `OrderPlaced` or `OrderCancelled` is dead-lettered, Payments does not act on it until an
operator replays it:
- for `OrderPlaced`, no `PaymentProcessed` or `PaymentFailed` is published;
- for `OrderCancelled`, no `PaymentRefunded` is published.

Replaying the stored payload to the original topic is safe, because handling is idempotent.
