# Contract: `payments.payment-refunded` (published)

Published by `PaymentEventPublisher.PublishRefundedAsync` only after the refund is durably
saved. **Message key**: the payment id (= `aggregateId`). JSON value:

```json
{
  "eventId": "0b7e5a4f-0000-4000-8000-000000000006",
  "occurredAt": "2026-11-12T10:05:01.123+00:00",
  "aggregateId": "7b0e8d4c-0000-4000-8000-000000000002",
  "version": 1,
  "orderId": "9d2b6e11-0000-4000-8000-000000000004",
  "amount": 129.99,
  "currency": "USD",
  "refundedAt": "2026-11-12T10:05:01.123+00:00"
}
```

| Field | Type | Meaning |
|---|---|---|
| `eventId` | uuid | New id for this event; unchanged if the event is later republished by the dead-letter retry worker. Consumers deduplicate by it. |
| `occurredAt` | ISO 8601 | When the refund happened (= `refundedAt`). |
| `aggregateId` | uuid | **The payment id.** |
| `version` | int | Schema version, `1`. |
| `orderId` | uuid | The order id. Downstream consumers MUST use this to identify the order (Constitution VI). |
| `amount` | decimal | Full amount refunded (the payment's amount). |
| `currency` | string | ISO 4217 code of the payment. |
| `refundedAt` | ISO 8601 | When the payment transitioned to `Refunded`. |

**Delivery**: at-least-once. If the publish fails after the save, the message is stored in
`payment_dead_letters` and republished unchanged (same key, value and `eventId`) by the US-03
retry worker ([docs/dead-letter-retry.md](../../../docs/dead-letter-retry.md)).

Breaking changes require `payments.payment-refunded.v2`.
