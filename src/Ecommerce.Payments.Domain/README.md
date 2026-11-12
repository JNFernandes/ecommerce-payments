# Ecommerce.Payments.Domain

**Layer**: Domain (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

Sole home of business logic. `Payment` is the aggregate root, with lifecycle
`Pending → Processed | Failed` and `Processed → Refunded`:

- `Payment.CreatePending(...)` — the only way a new `Payment` comes into existence; validates
  `amount`/`currency` and returns a new instance in `PaymentStatus.Pending`.
- `Payment.Evaluate(threshold)` — the amount-vs-threshold rule: calls `Process()` (raises
  `PaymentProcessed`) or `Fail(reason)` (raises `PaymentFailed`).
- `Payment.Refund(sourceEventId, refundedAt)` — `Processed → Refunded` (full amount), raises
  `PaymentRefunded`; throws `InvalidPaymentTransitionException` from any other state.
- `Payment.RequestRefund(sourceEventId, refundedAt)` — the refund rule used by the Service:
  returns `RefundDecision.Refunded(event)` for a Processed payment, or
  `RefundDecision.Ignored(NotCompleted | AlreadyRefunded)` otherwise. Never throws.
- `Payment.Rehydrate(...)` — persistence only: restores a saved aggregate without running rules.

Plain C# only — no dependency on EF Core, Confluent.Kafka, or ASP.NET/Worker types, so
Domain tests run with zero infrastructure. `PaymentStatus` is the only representation of
payment lifecycle state (no bare strings).

## Position in the flow

Invoked by `Ecommerce.Payments.Service`; never calls out to Repository or Publisher itself.
