# Ecommerce.Payments.Consumer

**Layer**: Consumer (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

The process entry point and host for the background services. `OrderPlacedConsumer` subscribes
to `orders.order-placed`, deserializes and validates each message into a typed
`OrderPlacedEvent` (rejecting malformed/incomplete envelopes before they ever reach
Domain), delegates to the Service layer (`ProcessPaymentHandler`) for a scoped unit of
work, and commits the Kafka offset only after that unit of work has fully succeeded.

Contains no business logic — see `Ecommerce.Payments.Service` for orchestration and
`Ecommerce.Payments.Domain` for business rules.

## Hosted services

| Service | Defined in | Responsibility |
|---|---|---|
| `OrderPlacedConsumer` | this project | Consumes `orders.order-placed` and runs the payment write flow. |
| `OrderCancelledConsumer` | this project | Consumes `orders.order-cancelled` and hands each cancellation to `RefundPaymentHandler` (US-04). |
| `OutboxRelayWorker` | `Ecommerce.Payments.Infrastructure/Outbox` | Publishes `payment_outbox` entries in order per payment, with backoff ([docs/outbox.md](../../docs/outbox.md), US-07). |
| `RetentionWorker` | `Ecommerce.Payments.Infrastructure/Retention` | Hourly purge of old published outbox entries, dead letters and cancellation markers ([docs/retention.md](../../docs/retention.md), US-09). |

## Topics

| Direction | Topic | Event |
|---|---|---|
| Consumes | `orders.order-placed` | `OrderPlaced` |
| Consumes | `orders.order-cancelled` | `OrderCancelled` |

Both consumers derive from `KafkaConsumerBase`, which does the following:
- **Commits** only after successful handling.
- **Retries** a failing message in place with capped exponential backoff
  (`Kafka:ConsumerRetryBaseDelayMs` / `Kafka:ConsumerRetryMaxDelayMs`), and never commits past it.
- **Dead-letters (US-06):** after `Kafka:ConsumerMaxAttempts` failed attempts, it stores the
  message as received in `consumed_dead_letters` through `IConsumedDeadLetterStore`, and commits
  only after that store has succeeded.
- **Malformed messages:** a consumer throws `InvalidMessageException` for a malformed envelope,
  which the base stores at once with `Attempts = 1` and never retries.
- **Store failures:** if the store fails, nothing is committed and the message is retried.
- **Startup:** it yields before touching Kafka, so startup never depends on the broker.

See [docs/consumed-dead-letters.md](../../docs/consumed-dead-letters.md) and
[ADR 0005](../../docs/adr/0005-inbound-dead-letter.md).

## Configuration

See `appsettings.json` — `ConnectionStrings:Payments`, `Kafka:BootstrapServers`,
`Kafka:ConsumerGroupId`, `Kafka:OrderPlacedTopic`, `Kafka:OrderCancelledTopic`,
`Kafka:PaymentProcessedTopic`, `Kafka:PaymentFailedTopic`, `Kafka:PaymentRefundedTopic`,
`Kafka:ConsumerRetryBaseDelayMs`, `Kafka:ConsumerRetryMaxDelayMs`, `Kafka:ConsumerMaxAttempts`
(the `Kafka` retry settings are validated at startup), `PaymentPolicy:MaxAmountThreshold`,
`Persistence:OrderLockTimeoutMs` (per-order lock wait, validated at startup; US-05), and the `OutboxRelay`
section (`PollIntervalMs`, `BatchSize`, `BaseDelayMs`, `MaxDelayMs`, `MaxRetries`), and the `Retention`
section (`IntervalMs`, `BatchSize`, `OutboxPublishedDays`, `ConsumedDeadLettersDays`,
`CancellationMarkersDays`; validated at startup, `0` days = keep forever; US-09).

## Running locally

Start the shared infrastructure from `ecommerce-infra` (see the root README), then:

```powershell
dotnet ef database update --project ../Ecommerce.Payments.Infrastructure --startup-project ../Ecommerce.Payments.Infrastructure
dotnet run
```
