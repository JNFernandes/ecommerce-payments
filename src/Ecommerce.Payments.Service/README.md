# Ecommerce.Payments.Service

**Layer**: Service (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

Orchestrates the end-to-end workflow for one consumed message, strictly in the order
Domain → Repository → Publisher, with a bounded retry (`PersistenceRetry`: 3 retries,
exponential from 200 ms) around the database calls for transient failures:

- **`ProcessPaymentHandler`** (`OrderPlaced`) — checks idempotency
  (`IPaymentRepository.ExistsByOrderIdAsync`), invokes Domain (`Payment.CreatePending()` →
  `Payment.Evaluate()`), persists via `IPaymentRepository.SaveAsync`, then publishes
  `PaymentProcessed` or `PaymentFailed`.
- **`RefundPaymentHandler`** (`OrderCancelled`, US-04) — loads the payment by order id
  (`GetByOrderIdAsync`), ignores a redelivery whose `eventId` already caused the refund, asks
  Domain to decide (`Payment.RequestRefund()`), saves with a conditional update
  (`SaveRefundAsync`, only if still Processed), then publishes `PaymentRefunded`. No payment,
  Failed/Pending, already Refunded, or a concurrent refund → one Information log, no error.

Inbound contracts live in `IntegrationEvents/`: `OrderPlacedEvent` and `OrderCancelledEvent`,
each with a `TryParse` envelope validator used by the Consumer.

Contains no business rules of its own (those belong to `Ecommerce.Payments.Domain`) and no
infrastructure code (`IPaymentRepository`/`IPaymentEventPublisher` are abstractions;
`Ecommerce.Payments.Infrastructure` implements them).

## Position in the flow

`OrderPlaced` / `OrderCancelled` (Consumer) → **this project** → `Payment` (Domain) →
`IPaymentRepository` / `IPaymentEventPublisher` (implemented in Infrastructure).
