# Contract: `OrderPlaced` (consumed)

**Topic**: `orders.order-placed`
**Direction**: Inbound (this service is a consumer)
**Owning service**: `orders` (upstream)

**Verified against the real producer**: `ecommerce-orders/src/domain/events/order-placed.event.ts`
(`OrderEventsProducer.publishOrderPlaced`, which does `JSON.stringify(event)` on the `OrderPlaced`
class). The shape below is the *actual* wire format, not an assumed minimum — the topic name
matched what was originally assumed, but the payload shape did not (see Assumption-check note
below).

## Payload (JSON)

```json
{
  "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "occurredAt": "2026-07-09T14:32:00Z",
  "aggregateId": "b3f1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
  "version": 1,
  "customerId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "items": [
    { "productId": "d1a2b3c4-5e6f-7a8b-9c0d-1e2f3a4b5c6d", "quantity": 2, "unitPrice": 64.995 }
  ],
  "totalAmount": 129.99
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `eventId` | UUID | yes | Unique id of this event instance. Used for log correlation. |
| `occurredAt` | string (ISO 8601 timestamp) | yes | When the order was placed. |
| `aggregateId` | UUID | yes | The order's aggregate id. **This is the order id** — there is no separate `orderId` field. |
| `version` | int | yes | Schema version of this event. `1` for this story. |
| `customerId` | UUID | yes | Customer being charged. |
| `items` | array | no (ignored) | Line items (`productId`, `quantity`, `unitPrice`). Not needed to charge the customer; this service reads `totalAmount` directly and does not recompute it from line items. |
| `totalAmount` | decimal | yes | Amount to charge. Must be > 0. **Not** `amount`. |

**No `currency` field exists anywhere in the orders service.** The platform is currently
single-currency. This service assumes `OrderPlacedEvent.DefaultCurrency` (`"USD"`) for every
`Payment` rather than reading a currency off the event. If the platform becomes multi-currency,
this is a breaking upstream schema change requiring a new contract version — not something this
service can infer on its own.

## Consumer validation rules

A message is rejected at the Consumer boundary (never reaches Domain) if:
- It is not valid JSON, or
- Any required field above is missing, or
- `totalAmount` is not a positive number, or
- `customerId` / `eventId` / `aggregateId` are not valid UUIDs.

Rejected messages are logged with full context and routed to review (dead-letter), not retried
as-is and not passed to the Service/Domain layers.

## Idempotency key

`aggregateId` (the order id). Redelivery of a message whose `aggregateId` already has a
`Payment` row is a no-op: no new row, no new `PaymentProcessed` publish.

## Assumption-check note (2026-07-09)

The original version of this contract (written before the `orders` service existed) assumed a
separate `orderId` field, an `amount` field, and a `currency` field. Once the real `orders`
service was available, none of those three assumptions held — the correction above was applied
retroactively to `OrderPlacedEvent.cs`, `ProcessPaymentHandler.cs`, and every test that
constructs an `OrderPlaced` payload. This is exactly the risk the original contract doc flagged
("align to the orders service's actual schema before implementation") — recorded here so the
next schema assumption gets checked against the real producer *before* writing tests against it,
not after.
