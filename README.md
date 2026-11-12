# ecommerce-payments

A .NET 10 event-driven background worker service that processes payments for the
`ecommerce` polyglot microservices system. It consumes `OrderPlaced` events from Kafka,
decides whether the payment can be processed, persists the outcome, and publishes the
result so other services can react.

No REST/GraphQL API — this is a pure Kafka consumer/producer worker.

## Architecture

Strict five-layer, one-way flow (see `.specify/memory/constitution.md` for the full rules and
[`docs/adr/`](docs/adr/README.md) for the architectural decisions behind them):

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

Payments is its own bounded context with its own PostgreSQL database: it has no `Order` model
and keeps only `orderId`, `customerId` and the amount from the event
([ADR 0001](docs/adr/0001-bounded-context-and-database-per-service.md)). It takes part in the
order/payment saga by choreography — there is no central orchestrator
([ADR 0002](docs/adr/0002-saga-by-choreography.md)).

**Write flow invariant:** the `Payment` row is always saved to PostgreSQL *before* any Kafka
event is published. If the DB save fails, the message is retried and nothing is published. If
the publish fails after a successful save, the event is written to a `payment_dead_letters`
table instead of being lost, and the offset is still committed.

Processing is idempotent: redelivering the same `OrderPlaced` message never creates a
duplicate `Payment` or a duplicate published event (the order's `aggregateId` is the
idempotency key).

## Event flow

```
ecommerce-orders                         ecommerce-payments
────────────────                         ──────────────────
OrderPlaced    ── orders.order-placed ──▶  create Payment → save → publish
                                            ├─ payments.payment-processed ──▶ Orders confirms order
                                            └─ payments.payment-failed    ──▶ Orders cancels order
OrderCancelled ── orders.order-cancelled ─▶ refund a Processed payment → save → publish
                                            └─ payments.payment-refunded
```

| Direction | Topic | Event | Status |
|---|---|---|---|
| Consumes | `orders.order-placed` | `OrderPlaced` | Implemented |
| Consumes | `orders.order-cancelled` | `OrderCancelled` | Implemented (US-04) |
| Publishes | `payments.payment-processed` | `PaymentProcessed` | Implemented |
| Publishes | `payments.payment-failed` | `PaymentFailed` | Implemented |
| Publishes | `payments.payment-refunded` | `PaymentRefunded` | Implemented (US-04) |

A `Payment` is created as `PENDING`, then evaluated against `PaymentPolicy.MaxAmountThreshold`:
amounts within the threshold are processed (`PROCESSED`); amounts over it fail
(`FAILED`, with a `reason`) so the `orders` service can compensate.

In inbound order events the order is identified by `aggregateId`. In every published payment
event `aggregateId` is the **payment** id and a separate `orderId` field carries the order id —
downstream consumers must use `orderId` to identify the order.

**Refunds (US-04):** an `OrderCancelled` for an order whose payment is `PROCESSED` marks it
`REFUNDED` (full amount) and publishes `PaymentRefunded`. Cancellations for `FAILED`, `PENDING`,
already `REFUNDED` or missing payments are ignored with an Information log and committed — the
normal "payment failed → order cancelled" saga path ends here. A redelivered cancellation is
recognised by its `eventId` (stored on the payment as `RefundSourceEventId`); a re-published
one with a new `eventId` by the `REFUNDED` state.

**Consumer retries:** both consumers share `KafkaConsumerBase`. If handling a message fails
(e.g. the database is down), the consumer retries the **same** message with exponential
backoff (`Kafka:ConsumerRetryBaseDelayMs` → `Kafka:ConsumerRetryMaxDelayMs`) and never commits
past it, so delivery stays at-least-once.

### Known limitations

- A message that fails permanently blocks its partition and retries indefinitely; there is no
  inbound dead-letter store yet (follow-up decision).
- If the process crashes after a refund (or payment) is saved but before its event is published
  or dead-lettered, that event is lost — there is no transactional outbox.
- An `OrderCancelled` that arrives before its payment exists is ignored; if the `OrderPlaced` is
  processed afterwards, that payment is not refunded automatically.
- `OrderPlacedConsumer` deduplicates by order id rather than by `eventId` (predates
  Constitution v1.1.0).

## Background services

| Hosted service | Responsibility | Status |
|---|---|---|
| `OrderPlacedConsumer` | Consumes `orders.order-placed` and runs the payment write flow. | Implemented |
| `DeadLetterRetryWorker` | Every `DeadLetterRetry:IntervalMs`, republishes due `payment_dead_letters` rows unchanged (same topic, key and `eventId`) with exponential backoff, marking them `Resolved` or `Exhausted` after `MaxRetries`; claims rows with `FOR UPDATE SKIP LOCKED` so several instances are safe. See [docs/dead-letter-retry.md](docs/dead-letter-retry.md) and [ADR 0003](docs/adr/0003-dead-letter-retry.md). | Implemented (US-03) |
| `OrderCancelledConsumer` | Consumes `orders.order-cancelled`; refunds the order's `Processed` payment and publishes `PaymentRefunded` (dead-lettered on publish failure). Failed, Pending, already Refunded or missing payments are ignored with an Information log. | Implemented (US-04) |

## Project layout

```
src/
  Ecommerce.Payments.Consumer/        Worker Service host, Kafka consumers
  Ecommerce.Payments.Service/         Application/handler layer
  Ecommerce.Payments.Domain/          Payment aggregate, domain events, business rules
  Ecommerce.Payments.Infrastructure/  EF Core repositories, Kafka publisher, dead-letter retry worker, migrations
tests/
  Ecommerce.Payments.Domain.Tests/          Unit tests, no infrastructure
  Ecommerce.Payments.Service.Tests/         Unit tests with mocked Repository/Publisher
  Ecommerce.Payments.Infrastructure.Tests/  Unit tests for dead-letter retry policy/processor/worker (fake clock)
  Ecommerce.Payments.Integration.Tests/  Consumer → Repository against Testcontainers Postgres
  Ecommerce.Payments.Component.Tests/    Full flow against Testcontainers Postgres + Kafka
docs/
  adr/                                Architecture Decision Records
  dead-letter-retry.md                Dead-letter states, settings, example SQL
specs/
  NNN-feature-name/                   Spec-Kit artifacts (spec, plan, tasks) per user story
```

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- Docker
- The sibling [`ecommerce-infra`](../ecommerce-infra) repository, which provides the shared
  local Kafka and PostgreSQL via docker compose

## Running locally against ecommerce-infra

Start the shared infrastructure from the `ecommerce-infra` repository (see its README):

```powershell
cd ../ecommerce-infra
docker compose up -d
```

This exposes Kafka on `localhost:9092` and the payments PostgreSQL on `localhost:5434`
(database `payments`, user `payments_user`, password `payments_pass`). The defaults in
`src/Ecommerce.Payments.Consumer/appsettings.json` already point at these.

Apply EF Core migrations (first run / after pulling new migrations):

```powershell
dotnet tool restore
dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure
```

The design-time factory reads `PAYMENTS_CONNECTION_STRING` if set, otherwise it uses the
`ecommerce-infra` defaults above.

Run the worker:

```powershell
dotnet run --project src/Ecommerce.Payments.Consumer
```

To point at a different database/broker, override via environment variables in the **same**
shell before running:

```powershell
$env:ConnectionStrings__Payments = "Host=localhost;Port=5434;Database=payments;Username=payments_user;Password=payments_pass"
$env:Kafka__BootstrapServers = "localhost:9092"
dotnet run --project src/Ecommerce.Payments.Consumer
```

### Docker image

```powershell
docker build -t ecommerce-payments .
```

Multi-stage build (.NET 10 SDK → .NET 10 runtime, non-root user). The service is not part of
any compose file; when running the container, pass `ConnectionStrings__Payments` and
`Kafka__BootstrapServers` pointing at addresses reachable from inside the container.

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
| `Kafka:OrderPlacedTopic` / `Kafka:OrderCancelledTopic` | Inbound topics |
| `Kafka:PaymentProcessedTopic` / `Kafka:PaymentFailedTopic` / `Kafka:PaymentRefundedTopic` | Outbound topics |
| `Kafka:ConsumerRetryBaseDelayMs` / `Kafka:ConsumerRetryMaxDelayMs` | Backoff before a consumer re-reads a message whose handling failed (defaults `1000` / `60000`) |
| `PaymentPolicy:MaxAmountThreshold` | Amount above which a payment fails instead of processing |
| `DeadLetterRetry:IntervalMs` | Time between dead-letter retry cycles (default `30000`) |
| `DeadLetterRetry:BatchSize` | Maximum rows retried per cycle (default `20`) |
| `DeadLetterRetry:BaseDelayMs` / `DeadLetterRetry:MaxDelayMs` | Exponential backoff base and cap (defaults `5000` / `600000`) |
| `DeadLetterRetry:MaxRetries` | Failed retries before a row is `Exhausted` (default `8`) |

Details: [docs/dead-letter-retry.md](docs/dead-letter-retry.md).

## Workflow

This repository follows the [Spec-Kit](.specify/) workflow: each user story is specified,
planned, broken into tasks, and implemented under `specs/NNN-feature-name/` before merging to
`main`. See `.specify/memory/constitution.md` for the non-negotiable architectural rules,
[`docs/adr/`](docs/adr/README.md) for architectural decisions, and `CLAUDE.md` for a condensed
guide.
