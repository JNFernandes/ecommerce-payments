# Data Model: Refund on Order Cancelled (US-04)

## Payment aggregate (Domain) — extended

| Member | Change |
|---|---|
| `PaymentStatus.Refunded` | **New** enum value. |
| `RefundedAt` (`DateTimeOffset?`) | **New**; set when refunded, null otherwise. |
| `RefundSourceEventId` (`Guid?`) | **New**; `eventId` of the `OrderCancelled` that caused the refund (FR-012). Also the `eventId` dedupe key: the handler ignores a delivery whose `eventId` equals it, before calling Domain (Constitution II). |
| `Refund(Guid sourceEventId, DateTimeOffset refundedAt)` → `PaymentRefunded` | **New** strict transition: `Processed → Refunded`; any other state throws `InvalidPaymentTransitionException`, state unchanged. |
| `RequestRefund(Guid sourceEventId, DateTimeOffset refundedAt)` → `RefundDecision` | **New**; the business decision used by the Service: `Refunded(event)` when `Processed`, `Ignored(AlreadyRefunded)` when `Refunded`, `Ignored(NotCompleted)` when `Failed` or `Pending`. Never throws. |
| `Rehydrate(...)` | **New** persistence-only factory restoring every field (used by the repository). |

### State transitions

```
Pending ──Process()──▶ Processed ──Refund()──▶ Refunded (terminal)
   └────Fail()──────▶ Failed (terminal)
```

## Domain types — new

- `PaymentRefunded : PaymentDomainEvent` — `PaymentId`, `OrderId`, `Amount`, `Currency`,
  `RefundedAt`.
- `RefundDecision` (abstract) with `RefundDecision.Refunded { PaymentRefunded Event }` and
  `RefundDecision.Ignored { RefundIgnoredReason Reason }`.
- `RefundIgnoredReason` enum: `NotCompleted`, `AlreadyRefunded`. (`NoPayment` is decided by the
  Service, which has no aggregate to ask.)

## Table `payments` — migration `AddPaymentRefundFields`

| Column | Type | Null | Notes |
|---|---|---|---|
| `RefundedAt` | timestamptz | yes | new |
| `RefundSourceEventId` | uuid | yes | new |
| `Status` | varchar(20) | no | existing; now may hold `Refunded` (fits 20 chars, no schema change) |

No backfill: existing rows are never refunded. `Down` drops both columns.

## Repository (`IPaymentRepository`) — extended

| Method | Behaviour |
|---|---|
| `GetByOrderIdAsync(Guid orderId)` → `Payment?` | Loads and rehydrates the payment, or null when none exists (lookup result, not an invariant). |
| `SaveRefundAsync(Payment payment)` → `bool` | Conditional update `WHERE Id = @id AND Status = 'Processed'` setting `Status`, `RefundedAt`, `RefundSourceEventId`; returns `false` if no row matched (refunded concurrently). |

## Integration events

- **Inbound** `OrderCancelledEvent` — see [contracts/orders.order-cancelled.md](contracts/orders.order-cancelled.md).
- **Outbound** `PaymentRefundedEvent` — see [contracts/payments.payment-refunded.md](contracts/payments.payment-refunded.md).

## Dead letters

No schema change. A failed `PaymentRefunded` publish creates a `payment_dead_letters` row with
`EventType = 'PaymentRefundedEvent'`, `Topic = 'payments.payment-refunded'`,
`MessageKey = payment id`, handled by the US-03 worker unchanged.

## Configuration (`Kafka` section) — extended

| Key | Default |
|---|---|
| `Kafka:OrderCancelledTopic` | `orders.order-cancelled` |
| `Kafka:PaymentRefundedTopic` | `payments.payment-refunded` |
| `Kafka:ConsumerRetryBaseDelayMs` | `1000` — first wait before re-consuming a failed message (both consumers) |
| `Kafka:ConsumerRetryMaxDelayMs` | `60000` — cap for that wait |
