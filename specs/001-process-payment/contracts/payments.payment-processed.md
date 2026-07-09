# Contract: `PaymentProcessed` (published)

**Topic**: `payments.payment-processed`
**Direction**: Outbound (this service is the producer)

## Payload (JSON)

```json
{
  "eventId": "9b2e4f1a-8c3d-4e5f-a6b7-c8d9e0f1a2b3",
  "occurredAt": "2026-07-09T14:32:01Z",
  "aggregateId": "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
  "version": 1,
  "orderId": "b3f1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
  "amount": 129.99,
  "currency": "USD",
  "processedAt": "2026-07-09T14:32:01Z"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `eventId` | UUID | yes | Newly generated id for this published event instance. |
| `occurredAt` | string (ISO 8601 timestamp) | yes | When this event was raised (equal to `processedAt` for this event type). |
| `aggregateId` | UUID | yes | The `Payment` aggregate id. |
| `version` | int | yes | Schema version. `1` for this story. |
| `orderId` | UUID | yes | The order this payment was for (correlates back to `OrderPlaced.orderId`). |
| `amount` | decimal | yes | Amount charged. |
| `currency` | string(3) | yes | ISO 4217 currency code. |
| `processedAt` | string (ISO 8601 timestamp) | yes | When the `Payment` transitioned to `Processed`. |

## Publishing rules

- Published **only** after the corresponding `Payment` row has been durably saved to PostgreSQL
  with `Status = Processed` (Constitution Principle II — non-negotiable ordering).
- Exactly one `PaymentProcessed` event per `Payment` row — never republished for the same
  `orderId` (see idempotency in [orders.order-placed.md](./orders.order-placed.md)).
- If publishing fails after the save succeeded, the event payload is written to the
  `payment_dead_letters` table (see [data-model.md](../data-model.md)) instead of being dropped;
  the consumer offset is still committed since the `Payment` state is already correct and
  durable. Replay of dead-lettered events is out of scope for this story.
