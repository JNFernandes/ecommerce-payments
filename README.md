# ecommerce-payments

A .NET 10 event-driven background worker service that processes payments for the
`ecommerce` polyglot microservices system. It consumes `OrderPlaced` events from Kafka,
decides whether the payment can be processed, persists the outcome, and publishes the
result so other services can react.

No REST/GraphQL API — this is a pure Kafka consumer/producer worker.

## Architecture

Strict five-layer, one-way flow (see `.specify/memory/constitution.md` for the full rules):

```
Consumer → Service → Domain → Repository → Publisher
```

| Layer | Project | Responsibility |
|---|---|---|
| Consumer | `Ecommerce.Payments.Consumer` | Kafka `IHostedService` consumers, deserializes `OrderPlaced`, delegates to Service, owns offset commits. |
| Service | `Ecommerce.Payments.Service` | Orchestrates Domain → Repository → Publisher, in that exact order. No business rules. |
| Domain | `Ecommerce.Payments.Domain` | `Payment` aggregate root: all business rules and state transitions (`Process()`, `Fail(reason)`, `Evaluate(...)`). Plain C#, no infrastructure dependencies. |
| Repository | `Ecommerce.Payments.Infrastructure/Persistence` | EF Core + PostgreSQL persistence behind `IPaymentRepository`. |
| Publisher | `Ecommerce.Payments.Infrastructure/Messaging` | Confluent.Kafka producer behind `IPaymentEventPublisher`. |

**Write flow invariant:** the `Payment` row is always saved to PostgreSQL *before* any Kafka
event is published. If the DB save fails, the message is retried and nothing is published. If
the publish fails after a successful save, the event is written to a `payment_dead_letters`
table instead of being lost, and the offset is still committed.

Processing is idempotent: redelivering the same `OrderPlaced` message never creates a
duplicate `Payment` or a duplicate published event.

## Events

| Direction | Topic | Event |
|---|---|---|
| Consumes | `orders.order-placed` | `OrderPlaced` |
| Publishes | `payments.payment-processed` | `PaymentProcessed` |
| Publishes | `payments.payment-failed` | `PaymentFailed` |

A `Payment` is created as `PENDING`, then evaluated against `PaymentPolicy.MaxAmountThreshold`:
amounts within the threshold are processed (`PROCESSED`); amounts over it fail
(`FAILED`, with a `reason`) so the `orders` service can compensate.

## Project layout

```
src/
  Ecommerce.Payments.Consumer/        Worker Service host, Kafka consumers
  Ecommerce.Payments.Service/         Application/handler layer
  Ecommerce.Payments.Domain/          Payment aggregate, domain events, business rules
  Ecommerce.Payments.Infrastructure/  EF Core repository, Kafka publisher, migrations
tests/
  Ecommerce.Payments.Domain.Tests/       Unit tests, no infrastructure
  Ecommerce.Payments.Service.Tests/      Unit tests with mocked Repository/Publisher
  Ecommerce.Payments.Integration.Tests/  Consumer → Repository against Testcontainers Postgres
  Ecommerce.Payments.Component.Tests/    Full flow against Testcontainers Postgres + Kafka
specs/
  NNN-feature-name/                   Spec-Kit artifacts (spec, plan, tasks) per user story
```

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- Docker (for PostgreSQL + Kafka, either via this repo's `docker-compose.yml` or a shared
  `ecommerce-infra` stack)

## Running locally

Start dependencies:

```powershell
docker compose up -d
```

This exposes PostgreSQL on `localhost:5432` and Kafka on `localhost:9092` (see
`docker-compose.yml`). Defaults in `src/Ecommerce.Payments.Consumer/appsettings.json` already
point at these ports.

Apply EF Core migrations (first run / after pulling new migrations):

```powershell
dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Consumer
```

Run the worker:

```powershell
dotnet run --project src/Ecommerce.Payments.Consumer
```

If you're pointing at a different database/broker (e.g. a shared `ecommerce-infra` stack with
non-default ports), override via environment variables in the **same** shell before running:

```powershell
$env:ConnectionStrings__Payments = "Host=localhost;Port=5434;Database=payments;Username=payments_user;Password=payments_pass"
$env:Kafka__BootstrapServers = "localhost:9192"
dotnet run --project src/Ecommerce.Payments.Consumer
```

## Testing

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

- Domain/Service tests run with no external dependencies.
- Integration/Component tests spin up disposable PostgreSQL and Kafka containers via
  Testcontainers — Docker must be running, no manual setup required.

## Configuration reference

`src/Ecommerce.Payments.Consumer/appsettings.json`:

| Key | Purpose |
|---|---|
| `ConnectionStrings:Payments` | PostgreSQL connection string |
| `Kafka:BootstrapServers` | Kafka broker address |
| `Kafka:ConsumerGroupId` | Consumer group id |
| `Kafka:OrderPlacedTopic` | Inbound topic |
| `Kafka:PaymentProcessedTopic` / `Kafka:PaymentFailedTopic` | Outbound topics |
| `PaymentPolicy:MaxAmountThreshold` | Amount above which a payment fails instead of processing |

## Workflow

This repository follows the [Spec-Kit](.specify/) workflow: each user story is specified,
planned, broken into tasks, and implemented under `specs/NNN-feature-name/` before merging to
`main`. See `.specify/memory/constitution.md` for the non-negotiable architectural rules.
