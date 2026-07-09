# Data Model: Process Payment from Order Placed Event

## Domain

### Payment (Aggregate Root)

The single aggregate for this bounded context. One `Payment` exists per order.

| Field | Type | Notes |
|---|---|---|
| `Id` | Guid | Payment aggregate id (`aggregateId` on outbound events). Generated on creation. |
| `OrderId` | Guid | The order this payment is for. **Unique** — enforces idempotency (see [research.md](./research.md#1-idempotency-enforcement)). |
| `CustomerId` | Guid | Customer being charged. |
| `Amount` | decimal | Charge amount, taken as-is from `OrderPlaced`. |
| `Currency` | string(3) | ISO 4217 currency code, taken as-is from `OrderPlaced`. |
| `Status` | `PaymentStatus` | `Pending` → `Processed` (this story); `Failed` exists on the enum for a future story (payment failure handling) but is not triggered by any flow in this story. |
| `SourceEventId` | Guid | The `OrderPlaced` event's `eventId`, kept for traceability/log correlation (not the idempotency key itself). |
| `CreatedAt` | DateTimeOffset | When the `Payment` record was first created (`Pending`). |
| `ProcessedAt` | DateTimeOffset? | When the transition to `Processed` completed. Null while `Pending`. |

**Entry point**: `Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId)` —
the only way a `Payment` comes into existence. Validates `amount`/`currency` and returns a new
aggregate in `Pending` with `Id` freshly generated and `CreatedAt` set. This satisfies FR-002
("create a payment record... starting in a pending state") independently of `Payment.Process()`,
which only ever transitions an *existing* `Payment`. The caller (`ProcessPaymentHandler`) sources
`orderId` from the inbound event's `aggregateId` and `currency` from a fixed platform default
(`"USD"`) — see the `OrderPlacedEvent` table below for why.

**Invariants**:
- `Status` only moves `Pending → Processed` in this story; any other requested transition
  (e.g., processing an already-`Processed` payment) throws `InvalidPaymentTransitionException`
  and MUST NOT mutate state.
- `Amount` MUST be greater than zero and `Currency` MUST be a non-empty 3-letter code; violations
  throw a domain exception rather than silently creating an invalid `Payment`. This is validated
  exactly once, by `Payment.Create()` — `Amount`/`Currency` are immutable after construction, and
  `Payment.Process()` takes no amount/currency input, so there is nothing for it to re-validate.
- No public setters on `Status`/`ProcessedAt` — only `Payment.Create()`, `Payment.Process()` (and,
  in a future story, `Payment.Fail(reason)`) may mutate them.

**State transitions** (this story):

```text
Create() --> Pending --Process()--> Processed
                |
                +--Process() on already-Processed--> throws InvalidPaymentTransitionException (no state change)
```

### PaymentStatus (enum)

`Pending`, `Processed`, `Failed` (`Failed` reserved for a future story; not reachable from any
flow implemented in this story).

### PaymentProcessed (Domain Event)

Raised in-process by `Payment.Process()` on a successful transition. Not yet a Kafka message at
this point — the Service layer maps it to the outbound integration event after a confirmed save.

| Field | Type |
|---|---|
| `PaymentId` | Guid |
| `OrderId` | Guid |
| `Amount` | decimal |
| `Currency` | string |
| `ProcessedAt` | DateTimeOffset |

## Integration Events (Kafka payloads)

See [contracts/](./contracts/) for the full wire-format definitions.

### OrderPlacedEvent (consumed, `orders.order-placed`)

Verified against the real producer (`ecommerce-orders`) — see
[contracts/orders.order-placed.md](./contracts/orders.order-placed.md) for the full note. There
is **no** separate `orderId` field (`aggregateId` is the order id) and **no** `currency` field
(the platform is single-currency; this service assumes `OrderPlacedEvent.DefaultCurrency`,
`"USD"`).

| Field | Type | Required |
|---|---|---|
| `eventId` | UUID | yes |
| `occurredAt` | ISO timestamp | yes |
| `aggregateId` | UUID — **the order id** | yes |
| `version` | int | yes |
| `customerId` | UUID | yes |
| `items` | array | no (present upstream, ignored here) |
| `totalAmount` | decimal — **not** `amount` | yes |

A message missing any required field, or with a field of the wrong shape, fails Consumer-level
validation (per spec Edge Cases) and is routed to review rather than reaching Domain.

### PaymentProcessedEvent (published, `payments.payment-processed`)

| Field | Type | Required |
|---|---|---|
| `eventId` | UUID | yes — newly generated for this outbound event |
| `occurredAt` | ISO timestamp | yes |
| `aggregateId` | UUID (payment id) | yes |
| `version` | int | yes |
| `orderId` | UUID | yes |
| `amount` | decimal | yes |
| `currency` | string(3) | yes |
| `processedAt` | ISO timestamp | yes |

## Persistence (Infrastructure)

### `payments` table (maps `Payment`)

Same fields as the `Payment` aggregate above, plus a unique index:

- `UNIQUE (order_id)` — the idempotency guarantee described in research.md.

### `payment_dead_letters` table

Holds events that failed to publish after a successful `Payment` save (see spec Edge Cases /
Constitution Principle II). Full retry/replay of this table is out of scope for this story
(tracked separately); this story only needs the write path so a post-save publish failure never
blocks the offset commit or loses the event.

| Field | Type | Notes |
|---|---|---|
| `Id` | Guid | Row id. |
| `PaymentId` | Guid | The `Payment` this event describes. |
| `EventType` | string | e.g., `PaymentProcessed`. |
| `Payload` | jsonb | The serialized integration event that failed to publish. |
| `FailureReason` | string | Exception message/summary for diagnostics. |
| `CreatedAt` | DateTimeOffset | When the publish failure was recorded. |
