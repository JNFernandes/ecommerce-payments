# Ecommerce.Payments.Domain

**Layer**: Domain (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

Sole home of business logic. `Payment` is the aggregate root:

- `Payment.CreatePending(...)` — the only way a `Payment` comes into existence; validates
  `amount`/`currency` and returns a new instance in `PaymentStatus.Pending`.
- `Payment.Process()` — transitions `Pending → Processed`, raises the `PaymentProcessed`
  domain event; throws `InvalidPaymentTransitionException` if not currently `Pending`.

Plain C# only — no dependency on EF Core, Confluent.Kafka, or ASP.NET/Worker types, so
Domain tests run with zero infrastructure. `PaymentStatus` is the only representation of
payment lifecycle state (no bare strings).

## Position in the flow

Invoked by `Ecommerce.Payments.Service`; never calls out to Repository or Publisher itself.
