# Contract: `PaymentFailed` (published)

**Topic**: `payments.payment-failed`
**Direction**: Outbound (this service is the producer)

## Payload (JSON)

```json
{
  "eventId": "9b2e4f1a-8c3d-4e5f-a6b7-c8d9e0f1a2b3",
  "occurredAt": "2026-07-09T14:32:01Z",
  "aggregateId": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
  "version": 1,
  "orderId": "b3f1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
  "reason": "Amount 15000.00 USD exceeds the maximum allowed threshold of 10000.00 USD.",
  "failedAt": "2026-07-09T14:32:01Z"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `eventId` | UUID | yes | Newly generated id for this published event instance. |
| `occurredAt` | string (ISO 8601 timestamp) | yes | When this event was raised (equal to `failedAt` for this event type). |
| `aggregateId` | UUID | yes | The `Payment` aggregate id. |
| `version` | int | yes | Schema version. `1` for this story. |
| `orderId` | UUID | yes | The order this payment was for (correlates back to `OrderPlaced.aggregateId`). |
| `reason` | string | yes | Human-readable business reason the payment could not be completed. No fixed taxonomy (see spec Assumptions) — currently always references the amount threshold, but the field is free text so future failure conditions don't require a schema change. |
| `failedAt` | string (ISO 8601 timestamp) | yes | When the `Payment` transitioned to `Failed`. |

## Publishing rules

- Published **only** after the corresponding `Payment` row has been durably saved to PostgreSQL
  with `Status = Failed` (Constitution Principle II — same non-negotiable ordering as
  `PaymentProcessed`).
- Exactly one `PaymentFailed` event per failed `Payment` row — never republished for the same
  `orderId` (idempotency is the same `order_id` unique-index mechanism described in
  [orders.order-placed.md](../../001-process-payment/contracts/orders.order-placed.md)).
- If publishing fails after the save succeeded, the event payload is written to
  `payment_dead_letters` instead of being dropped, identically to `PaymentProcessed`'s existing
  fallback (see [data-model.md](../data-model.md)); the consumer offset is still committed.
- A business failure is never accompanied by a `PaymentProcessed` event for the same order, and
  vice versa — `Payment.Evaluate()` produces exactly one outcome per order.
