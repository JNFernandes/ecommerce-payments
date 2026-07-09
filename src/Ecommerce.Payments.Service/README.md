# Ecommerce.Payments.Service

**Layer**: Service (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

Orchestrates the end-to-end workflow for one consumed `OrderPlaced` message:
`ProcessPaymentHandler` checks idempotency (`IPaymentRepository.ExistsByOrderIdAsync`),
invokes Domain (`Payment.CreatePending()` → `Payment.Process()`), persists via
`IPaymentRepository.SaveAsync`, then publishes via `IPaymentEventPublisher.PublishAsync` —
strictly in that order, with a bounded retry around the database calls for transient
failures.

Contains no business rules of its own (those belong to `Ecommerce.Payments.Domain`) and no
infrastructure code (`IPaymentRepository`/`IPaymentEventPublisher` are abstractions;
`Ecommerce.Payments.Infrastructure` implements them).

## Position in the flow

`OrderPlaced` (Consumer) → **this project** → `Payment` (Domain) → `IPaymentRepository` /
`IPaymentEventPublisher` (implemented in Infrastructure).
