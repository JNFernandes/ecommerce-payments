# Ecommerce.Payments.Infrastructure

**Layers**: Repository + Publisher (per the
[Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow), plus the dead-letter retry worker

## Responsibility

Outward-facing adapters, kept in separate folders/namespaces and never referenced from Domain:

- **`Persistence/`** (Repository) — `PaymentsDbContext` (EF Core + PostgreSQL),
  `PaymentEntity`/`PaymentDeadLetterEntity` (persistence models, mapped explicitly to/from
  the `Payment` domain aggregate — EF Core types never leak into Domain),
  `PaymentRepository` (implements `IPaymentRepository`; the `payments.order_id` unique
  index is the durable idempotency guarantee), and `PaymentDeadLetterRepository`
  (implements `IPaymentDeadLetterRepository`; the single owner of `payment_dead_letters`,
  including the `FOR UPDATE SKIP LOCKED` claim query).
- **`Messaging/`** (Publisher) — `PaymentEventPublisher` (implements
  `IPaymentEventPublisher` and `IRawEventPublisher`; Confluent.Kafka producer). On a publish
  failure *after* a successful save, it writes the event to `payment_dead_letters` (with its
  topic, key and retry bookkeeping) instead of losing it, rather than rolling back the
  already-correct `Payment` row. `IRawEventPublisher.PublishRawAsync` publishes an
  already-serialized message and throws on failure without dead-lettering — used by both the
  normal path and the retry worker.
- **`DeadLetters/`** — `DeadLetterRetryWorker` (hosted service, registered by the Consumer
  host), `DeadLetterRetryProcessor` (one cycle: claim → republish → update, one transaction
  per row), `DeadLetterRetryPolicy` (pure backoff/exhaustion rules) and
  `DeadLetterRetryOptions` (`DeadLetterRetry` config section). See
  [docs/dead-letter-retry.md](../../docs/dead-letter-retry.md) and
  [ADR 0003](../../docs/adr/0003-dead-letter-retry.md).

## Topics

| Direction | Topic | Event |
|---|---|---|
| Publishes | `payments.payment-processed` | `PaymentProcessed` |
| Publishes | `payments.payment-failed` | `PaymentFailed` |

Dead-lettered events are republished to the same topic, unchanged.

## Migrations

```powershell
dotnet ef migrations add <Name> --project . --startup-project .
dotnet ef database update --project . --startup-project .
```
