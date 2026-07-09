# Ecommerce.Payments.Infrastructure

**Layers**: Repository + Publisher (per the
[Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

Two outward-facing adapters, kept in separate folders/namespaces and never referenced from
Domain:

- **`Persistence/`** (Repository) — `PaymentsDbContext` (EF Core + PostgreSQL),
  `PaymentEntity`/`PaymentDeadLetterEntity` (persistence models, mapped explicitly to/from
  the `Payment` domain aggregate — EF Core types never leak into Domain), and
  `PaymentRepository` (implements `IPaymentRepository`; the `payments.order_id` unique
  index is the durable idempotency guarantee).
- **`Messaging/`** (Publisher) — `PaymentEventPublisher` (implements
  `IPaymentEventPublisher`; Confluent.Kafka producer). On a publish failure *after* a
  successful save, it writes the event to `payment_dead_letters` instead of losing it,
  rather than rolling back the already-correct `Payment` row.

## Topics

| Direction | Topic | Event |
|---|---|---|
| Publishes | `payments.payment-processed` | `PaymentProcessed` |

## Migrations

```powershell
dotnet ef migrations add <Name> --project . --startup-project .
dotnet ef database update --project . --startup-project .
```
