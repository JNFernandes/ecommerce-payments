# Ecommerce.Payments.Service

**Layer**: Service (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

Orchestrates the end-to-end workflow for one consumed message, strictly in the order
Domain → Repository (payment change **and** its outbox entry, one transaction), with a bounded retry (`PersistenceRetry`: 3 retries,
exponential from 200 ms) around the database calls for transient failures:

- **`ProcessPaymentHandler`** (`OrderPlaced`) — checks idempotency
  (`IPaymentRepository.ExistsByOrderIdAsync`), invokes Domain (`Payment.CreatePending()` →
  `Payment.Evaluate()`), persists via `IPaymentRepository.SaveAsync` and records
  `PaymentProcessed` or `PaymentFailed` through `IPaymentOutbox`, in the same transaction.
- **`RefundPaymentHandler`** (`OrderCancelled`, US-04) — loads the payment by order id
  (`GetByOrderIdAsync`), ignores a redelivery whose `eventId` already caused the refund, asks
  Domain to decide (`Payment.RequestRefund()`), saves with a conditional update
  (`SaveRefundAsync`, only if still Processed) and records `PaymentRefunded` through `IPaymentOutbox`. Failed or
  Pending, already Refunded, or a concurrent refund → one Information log, no error. No payment
  → a cancellation marker (US-05, below).

**Per-order unit of work (US-05):** both handlers run their read-decide-write step inside
`IOrderUnitOfWork.BeginAsync(orderId)` (`Orders/`), a transaction that holds a per-order lock.
`PersistenceRetry` retries the whole unit ([ADR 0004](../../docs/adr/0004-per-order-serialisation.md)). Handlers never
publish: the outbox relay does, after the commit (US-07, [ADR 0006](../../docs/adr/0006-transactional-outbox.md)).
- `RefundPaymentHandler`: when no payment exists, it records a `CancellationMarker` through
  `ICancellationMarkerRepository`. A duplicate `eventId`, or an order that already has a marker,
  is logged and nothing changes.
- `ProcessPaymentHandler`: for an order with a marker, it creates no payment, publishes nothing
  and logs one Information line.

Inbound contracts live in `IntegrationEvents/`: `OrderPlacedEvent` and `OrderCancelledEvent`,
each with a `TryParse` envelope validator used by the Consumer.

Contains no business rules of its own (those belong to `Ecommerce.Payments.Domain`) and no
infrastructure code (`IPaymentRepository`, `IPaymentOutbox` and the `Orders/` ports are abstractions;
`Ecommerce.Payments.Infrastructure` implements them).

## Position in the flow

`OrderPlaced` / `OrderCancelled` (Consumer) → **this project** → `Payment` (Domain) →
`IPaymentRepository` / `IPaymentOutbox` (implemented in Infrastructure; the outbox relay publishes).
