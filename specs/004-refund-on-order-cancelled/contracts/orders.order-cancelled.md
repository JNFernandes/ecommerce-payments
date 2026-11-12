# Contract: `orders.order-cancelled` (consumed)

Producer: `ecommerce-orders` (`src/domain/events/order-cancelled.event.ts`). Consumer:
`OrderCancelledConsumer` → `OrderCancelledEvent.TryParse`.

```json
{
  "eventId": "6f1d2c3b-0000-4000-8000-000000000001",
  "occurredAt": "2026-11-12T10:05:00.000Z",
  "aggregateId": "9d2b6e11-0000-4000-8000-000000000004",
  "version": 1,
  "customerId": "1a2b3c4d-0000-4000-8000-000000000005",
  "reason": "Payment failed: amount exceeds threshold"
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `eventId` | uuid | yes | Id of this publish attempt (new on every publish). Logged; stored on the payment as `RefundSourceEventId` when it causes a refund. |
| `occurredAt` | ISO 8601 | yes | When the order was cancelled. |
| `aggregateId` | uuid | yes | **The order id** — used to find the payment. |
| `version` | int > 0 | yes | Schema version (1). |
| `customerId` | uuid | no | Not used by Payments. |
| `reason` | string | no | Logged only; present today for payment-failure cancellations. |

Unknown fields are ignored. A message that is not valid JSON or lacks a required field is
logged as a warning and committed (never reaches the Service layer).

## Outcomes

| Payment for `aggregateId` | Result | Offset |
|---|---|---|
| any, with `RefundSourceEventId` = this `eventId` | duplicate delivery ignored before Domain, Information log | committed |
| `Processed` | Refunded, `PaymentRefunded` published (or dead-lettered) | committed |
| `Refunded` | ignored, Information log "already refunded" | committed |
| `Failed` / `Pending` | ignored, Information log "payment not completed" | committed |
| none | ignored, Information log "no payment for order" | committed |
| DB failure after retries | Error log; the consumer seeks back and retries the same message with capped exponential backoff | not committed (nothing after it is committed either) |
