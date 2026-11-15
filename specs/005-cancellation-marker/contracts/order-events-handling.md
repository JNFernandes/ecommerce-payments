# Contract: Handling of Order Events (US-05)

US-05 adds **no new topics and no new payloads**. The inbound contracts are unchanged:

- [`orders.order-placed`](../../001-process-payment/contracts/orders.order-placed.md) (US-01)
- [`orders.order-cancelled`](../../004-refund-on-order-cancelled/contracts/orders.order-cancelled.md) (US-04)

The outbound contracts are also unchanged: `payments.payment-processed`,
`payments.payment-failed` and `payments.payment-refunded`.

What changes is the **observable behaviour** for one ordering of the two inbound events.

## Behaviour promised to Orders

| Arrival order | Payments publishes |
|---|---|
| `OrderPlaced` → `OrderCancelled` | `PaymentProcessed` then `PaymentRefunded`, **or** `PaymentFailed` only (US-04, unchanged) |
| `OrderCancelled` → `OrderPlaced` | **nothing**, for that order, ever |
| concurrent | one of the two rows above, never a `PaymentProcessed` without a following `PaymentRefunded` |

- Orders must not wait for a payment outcome for an order it has already cancelled.
- Redelivering either event never changes the result.
- A malformed `OrderCancelled` is still rejected before any business logic. It is logged as a
  warning, its offset is committed, and no marker is recorded.

## Offsets

Offsets are committed only after the locked transaction has committed, or after a decision to
skip has been made under the lock. If the database stays unavailable, the offset is not
committed and the consumer retries the message in place (US-04 `KafkaConsumerBase`).
