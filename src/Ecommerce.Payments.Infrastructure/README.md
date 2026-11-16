# Ecommerce.Payments.Infrastructure

**Layers**: Repository + Publisher (per the
[Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow), plus the outbox relay

## Responsibility

Outward-facing adapters, kept in separate folders/namespaces and never referenced from Domain:

- **`Persistence/`** (Repository) — `PaymentsDbContext` (EF Core + PostgreSQL),
  `PaymentEntity`/`OutboxEntryEntity` (persistence models, mapped explicitly to/from
  the `Payment` domain aggregate — EF Core types never leak into Domain),
  `PaymentRepository` (implements `IPaymentRepository`; the `payments.order_id` unique
  index is the durable idempotency guarantee; `GetByOrderIdAsync` rehydrates the aggregate and `SaveRefundAsync` is a conditional update that only succeeds while the payment is still Processed), and `PaymentOutboxRepository`
  (implements `IPaymentOutboxRepository`; relay-side access to `payment_outbox`,
  including the per-payment ordered `FOR UPDATE SKIP LOCKED` claim query).

  US-05 adds the following:
  - `CancellationMarkerEntity` and `CancellationMarkerRepository`: the
    `order_cancellation_markers` table, with primary key `OrderId`, so there is one marker per
    order.
  - `OrderUnitOfWork`, which implements the Service port `IOrderUnitOfWork`. It opens a
    transaction, sets a local `lock_timeout` (`PersistenceOptions.OrderLockTimeoutMs`) and takes
    `pg_advisory_xact_lock` for the order ([ADR 0004](../../docs/adr/0004-per-order-serialisation.md)).
    On rollback it clears the change tracker so a retry starts clean.
  - `AddOrderPersistence()`, which registers both.

  US-06 adds the following:
  - `ConsumedDeadLetterEntity`, mapped to the `consumed_dead_letters` table. The table has a
    unique index on (Topic, Partition, Offset) and a check that `Attempts` is at least 1.
  - `IConsumedDeadLetterStore` and `ConsumedDeadLetterStore`, an idempotent
    `INSERT … ON CONFLICT DO NOTHING`.
  - `AddConsumedDeadLetterStore()`, which registers the store.

  The Consumer host's `KafkaConsumerBase` uses the store for inbound messages that are malformed
  or keep failing ([docs/consumed-dead-letters.md](../../docs/consumed-dead-letters.md)).
- **`Messaging/`** (Publisher): `PaymentEventPublisher` implements `IRawEventPublisher` with
  the Confluent.Kafka producer (`MessageTimeoutMs` 10 s). It publishes an already-serialized
  outbox entry unchanged and throws on failure. Only the outbox relay calls it (US-07).
- **`Outbox/`**: the transactional outbox (US-07,
  [ADR 0006](../../docs/adr/0006-transactional-outbox.md), [docs/outbox.md](../../docs/outbox.md)).
  - `PaymentOutbox` implements the Service port `IPaymentOutbox`. It builds and serializes the
    integration event (new `eventId`) and adds a `Pending` entry to `payment_outbox` in the
    caller's per-order transaction. It is registered with `AddPaymentOutbox()`.
  - `OutboxRelayWorker` is the hosted service, registered by the Consumer host.
  - `OutboxRelayProcessor` runs one cycle: claim, then publish, then update, one transaction
    per entry, in order per payment.
  - `OutboxRetryPolicy` holds the pure backoff and exhaustion rules.
  - `OutboxRelayOptions` maps the `OutboxRelay` config section.

- **`Retention/`**: data retention (US-09, [ADR 0007](../../docs/adr/0007-data-retention.md),
  [docs/retention.md](../../docs/retention.md)).
  - `RetentionPurger` implements `IRetentionPurger`. It deletes `Published` outbox entries,
    inbound dead letters and cancellation markers older than their retention periods. Each batch
    is one statement, `DELETE … IN (SELECT … LIMIT n FOR UPDATE SKIP LOCKED)`.
  - `RetentionWorker` is the hosted service; the Consumer host runs it every hour.
  - `RetentionOptions` binds the `Retention` config section.
  - `PurgeResult` holds the counts deleted per table.
  - `AddRetention()` registers the purger.

  Migration `AddRetentionIndexes` adds the three age indexes the purge uses.

## Topics

| Direction | Topic | Event |
|---|---|---|
| Publishes | `payments.payment-processed` | `PaymentProcessed` |
| Publishes | `payments.payment-failed` | `PaymentFailed` |
| Publishes | `payments.payment-refunded` | `PaymentRefunded` |

Every event is recorded in `payment_outbox` with its payment change and published by the relay, unchanged.

## Migrations

```powershell
dotnet ef migrations add <Name> --project . --startup-project .
dotnet ef database update --project . --startup-project .
```
