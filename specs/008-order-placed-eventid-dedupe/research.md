# Research: OrderPlaced eventId Dedupe (US-08)

Every decision below is made; nothing is left as NEEDS CLARIFICATION.

## R1. Where the event identifier comes from

**Decision**: Use `Payment.SourceEventId`. It is set by `Payment.CreatePending(..., sourceEventId)`
from the `OrderPlaced` `eventId`, persisted since US-01, and restored by `Payment.Rehydrate`.

**Rationale**: No new storage is needed, and it works for existing payments. This mirrors US-04,
which compares `RefundSourceEventId` with the `OrderCancelled` `eventId` before the Domain runs.

**Alternatives considered**: an inbox (`processed_events`) table, as Orders and Inventory use.
It would duplicate data that the payment already stores, and the user decided against an inbox
in US-04.

## R2. How the handler reads the payment

**Decision**: replace `IPaymentRepository.ExistsByOrderIdAsync` with the existing
`GetByOrderIdAsync`, which returns `Payment?` as an `AsNoTracking` read followed by `Rehydrate`.
Inside the per-order transaction, the handler then does the following:

| Payment found | Condition | Result |
|---|---|---|
| yes | `payment.SourceEventId == orderPlaced.EventId` | `Outcome.DuplicateDelivery(payment.Id)` |
| yes | different `eventId` | `Outcome.AlreadyExists(payment.Id, payment.SourceEventId)` |
| no | the order has a marker | `Outcome.OrderCancelled` (unchanged) |
| no | no marker | create the payment, as today |

`ExistsByOrderIdAsync` is then unused, so it is removed from the interface and the
implementation.

**Rationale**:
- It is still a single indexed read, using the unique index on `order_id`.
- It returns the identifiers needed for the log lines.
- It avoids two methods that answer almost the same question.

**Alternatives considered**: keep `ExistsByOrderIdAsync` and add a second query. That adds an
extra round trip and duplicates the method's purpose.

## R3. Logging

**Decision**: write one Information line per ignored delivery, after the transaction:
- "Ignoring OrderPlaced {EventId} for order {OrderId}: duplicate delivery; payment {PaymentId}
  was created by it."
- "Ignoring OrderPlaced {EventId} for order {OrderId}: payment {PaymentId} already exists
  (created by OrderPlaced {SourceEventId})."

The wording matches `RefundPaymentHandler`'s "duplicate delivery" line, so operators see the
same pattern for both consumers.

## R4. Tests

**Unit tests** (`ProcessPaymentHandlerTests`):
- Same `eventId`: there is no save and no outbox entry; one Information line contains
  "duplicate delivery", the order, the event and the payment.
- Different `eventId`: there is no save and no outbox entry; one Information line contains
  "already exists" and the source event.
- Existing tests that set up `ExistsByOrderIdAsync` switch to `GetByOrderIdAsync`, returning
  `null` or a rehydrated payment.

**Integration tests** (`OrderPlacedToPaymentTests`, real PostgreSQL):
- The same raw message is handled twice through the consumer. There is one payment and one
  outbox entry.
- A second message for the same order with a new `eventId` also leaves one payment and one
  entry.

**Component tests**: the `DuplicateDeliveryTests` already cover a redelivery end to end, and
stay green unchanged.
