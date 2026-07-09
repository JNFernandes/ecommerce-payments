# Ecommerce.Payments.Consumer

**Layer**: Consumer (per the [Constitution](../../.specify/memory/constitution.md)'s
Consumer → Service → Domain → Repository → Publisher flow)

## Responsibility

The process entry point and Kafka consumer host. `OrderPlacedConsumer` subscribes to
`orders.order-placed`, deserializes and validates each message into a typed
`OrderPlacedEvent` (rejecting malformed/incomplete envelopes before they ever reach
Domain), delegates to the Service layer (`ProcessPaymentHandler`) for a scoped unit of
work, and commits the Kafka offset only after that unit of work has fully succeeded.

Contains no business logic — see `Ecommerce.Payments.Service` for orchestration and
`Ecommerce.Payments.Domain` for business rules.

## Topics

| Direction | Topic | Event |
|---|---|---|
| Consumes | `orders.order-placed` | `OrderPlaced` |

## Configuration

See `appsettings.json` — `Kafka:BootstrapServers`, `Kafka:ConsumerGroupId`,
`Kafka:OrderPlacedTopic`, `Kafka:PaymentProcessedTopic`, and
`ConnectionStrings:Payments`.

## Running locally

```powershell
docker-compose up -d postgres kafka
dotnet ef database update --project ../Ecommerce.Payments.Infrastructure --startup-project .
dotnet run
```
