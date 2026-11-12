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
| `DeadLetterRetryWorker` | `Ecommerce.Payments.Infrastructure/DeadLetters` | Republishes `payment_dead_letters` rows with backoff ([docs/dead-letter-retry.md](../../docs/dead-letter-retry.md)). |

## Topics

| Direction | Topic | Event |
|---|---|---|
| Consumes | `orders.order-placed` | `OrderPlaced` |

## Configuration

See `appsettings.json` — `ConnectionStrings:Payments`, `Kafka:BootstrapServers`,
`Kafka:ConsumerGroupId`, `Kafka:OrderPlacedTopic`, `Kafka:PaymentProcessedTopic`,
`Kafka:PaymentFailedTopic`, `PaymentPolicy:MaxAmountThreshold`, and the `DeadLetterRetry`
section (`IntervalMs`, `BatchSize`, `BaseDelayMs`, `MaxDelayMs`, `MaxRetries`).

## Running locally

Start the shared infrastructure from `ecommerce-infra` (see the root README), then:

```powershell
dotnet ef database update --project ../Ecommerce.Payments.Infrastructure --startup-project ../Ecommerce.Payments.Infrastructure
dotnet run
```
