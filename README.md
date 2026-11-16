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
| Service | `Ecommerce.Payments.Service` | Orchestrates Domain → Repository: saves the payment change and its outbox entry in one transaction; never publishes. No business rules. |
| Domain | `Ecommerce.Payments.Domain` | `Payment` aggregate root: all business rules and state transitions (`Process()`, `Fail(reason)`, `Evaluate(...)`). Plain C#, no infrastructure dependencies. |
| Repository | `Ecommerce.Payments.Infrastructure/Persistence` | EF Core + PostgreSQL persistence behind `IPaymentRepository` and `IPaymentOutbox` (`payment_outbox`). |
| Publisher | `Ecommerce.Payments.Infrastructure/Messaging` | Confluent.Kafka producer behind `IRawEventPublisher`, called only by the outbox relay (US-07). |

Payments is its own bounded context with its own PostgreSQL database: it has no `Order` model
and keeps only `orderId`, `customerId` and the amount from the event
([ADR 0001](docs/adr/0001-bounded-context-and-database-per-service.md)). It takes part in the
order/payment saga by choreography — there is no central orchestrator
([ADR 0002](docs/adr/0002-saga-by-choreography.md)).

**Write flow invariant (transactional outbox, US-07):**
- The payment change and its event (`PaymentProcessed`, `PaymentFailed` or `PaymentRefunded`)
  are saved to PostgreSQL in **one transaction**. The event goes to the `payment_outbox` table.
- The handler never publishes. The offset is committed once that transaction has committed.
- The **outbox relay** then publishes the recorded event unchanged, retrying with backoff if
  Kafka is unavailable.
- If the save fails, nothing is saved, nothing is published, and the message is retried.
- A crash at any point loses no event, and events for one payment are published in the order
  they happened.

See [docs/outbox.md](docs/outbox.md) and [ADR 0006](docs/adr/0006-transactional-outbox.md).

Processing is idempotent (US-08):
- A redelivered `OrderPlaced` is recognised by its `eventId`, which is stored on the payment as
  `SourceEventId` and checked before any business logic. It is logged as a duplicate delivery.
- Any other `OrderPlaced` for an order that already has a payment is ignored too: the order's
  `aggregateId` keeps it to one payment per order.
- Neither case creates a payment or records an event.

## Event flow

```
ecommerce-orders                         ecommerce-payments
────────────────                         ──────────────────
OrderPlaced    ── orders.order-placed ──▶  create Payment → save payment + outbox entry ─┐
OrderCancelled ── orders.order-cancelled ─▶ refund a Processed payment → save + entry ─┤
                                                                                        ▼
                                            outbox relay publishes, in order per payment:
                                            ├─ payments.payment-processed ──▶ Orders confirms order
                                            ├─ payments.payment-failed    ──▶ Orders cancels order
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
`REFUNDED` (full amount) and publishes `PaymentRefunded`.
- Cancellations for `FAILED`, `PENDING` or already `REFUNDED` payments are ignored with an
  Information log and committed. The normal "payment failed → order cancelled" saga path ends
  here.
- A redelivered cancellation is recognised by its `eventId`, stored on the payment as
  `RefundSourceEventId`.
- A re-published cancellation with a new `eventId` is recognised by the `REFUNDED` state.

**Cancellation before payment (US-05):** the two inbound topics are not ordered, so an
`OrderCancelled` can arrive before its `OrderPlaced`.
- In that case Payments records a **cancellation marker** in `order_cancellation_markers`, one
  per order.
- A later `OrderPlaced` for a marked order creates no payment and publishes neither
  `PaymentProcessed` nor `PaymentFailed`. Orders has already cancelled the order.

Both handlers do their read-decide-write step inside one transaction that first takes a
per-order PostgreSQL advisory lock. When both events for an order are handled at once, the one
that commits first wins and the other sees it
([ADR 0004](docs/adr/0004-per-order-serialisation.md)). The lock wait is bounded by
`Persistence:OrderLockTimeoutMs`.

**Consumer retries and inbound dead letters (US-06):** both consumers share `KafkaConsumerBase`.
- **Failed handling** (for example, the database is down): the consumer retries the **same**
  message with exponential backoff (`Kafka:ConsumerRetryBaseDelayMs` →
  `Kafka:ConsumerRetryMaxDelayMs`) and never commits past it.
- **Still failing after `Kafka:ConsumerMaxAttempts`** (default 5): the message is stored as
  received in `consumed_dead_letters` and logged as an Error. Its offset is committed only after
  that, so the partition moves on.
- **Malformed message:** stored at once with `Attempts = 1`.
- **Store fails:** nothing is committed and the message keeps being retried, so delivery stays
  at-least-once.

How to list and replay stored messages:
[docs/consumed-dead-letters.md](docs/consumed-dead-letters.md)
([ADR 0005](docs/adr/0005-inbound-dead-letter.md)).

### Known limitations

- Messages in `consumed_dead_letters` are not replayed automatically, and the service never
  removes them. A parked `OrderCancelled` leaves its payment `Processed` until it is replayed.
- The consumer keeps its attempt count in memory, so after a restart the count for a failing
  message starts again.
- An `Exhausted` outbox entry holds back later events for the same payment until it is re-queued
  ([docs/outbox.md](docs/outbox.md)). Published outbox entries are kept and never cleaned up.
- Upgrading to US-07: stop the service, run the migration (`payment_dead_letters` becomes
  `payment_outbox`), then start it. The `DeadLetterRetry` settings section is now `OutboxRelay`.
- Cancellation markers are never expired. Early cancellations that US-04 ignored before US-05
  shipped are not backfilled.

## Background services

| Hosted service | Responsibility | Status |
|---|---|---|
| `OrderPlacedConsumer` | Consumes `orders.order-placed` and runs the payment write flow. It skips orders that have a cancellation marker (US-05). Messages that are malformed or keep failing go to `consumed_dead_letters` (US-06). | Implemented |
| `OutboxRelayWorker` | Every `OutboxRelay:PollIntervalMs` (250 ms) it publishes due `payment_outbox` entries unchanged (same topic, key and `eventId`), in recording order per payment. It retries failures with exponential backoff and marks an entry `Exhausted` after `MaxRetries`. Entries are claimed with `FOR UPDATE SKIP LOCKED`, so several instances are safe. See [docs/outbox.md](docs/outbox.md) and [ADR 0006](docs/adr/0006-transactional-outbox.md). | Implemented (US-03, US-07) |
| `OrderCancelledConsumer` | Consumes `orders.order-cancelled`. Refunds the order's `Processed` payment and records `PaymentRefunded` in the outbox with the refund (US-07). Failed, Pending or already Refunded payments are ignored with an Information log. With no payment, it records a cancellation marker (US-05). Messages that are malformed or keep failing go to `consumed_dead_letters` (US-06). | Implemented (US-04, US-05, US-06) |

## Project layout

```
src/
  Ecommerce.Payments.Consumer/        Worker Service host, Kafka consumers
  Ecommerce.Payments.Service/         Application/handler layer
  Ecommerce.Payments.Domain/          Payment aggregate, domain events, business rules
  Ecommerce.Payments.Infrastructure/  EF Core repositories, outbox and its relay, Kafka publisher, migrations
tests/
  Ecommerce.Payments.Domain.Tests/          Unit tests, no infrastructure
  Ecommerce.Payments.Service.Tests/         Unit tests with mocked Repository/Publisher
  Ecommerce.Payments.Infrastructure.Tests/  Unit tests for dead-letter retry policy/processor/worker (fake clock)
  Ecommerce.Payments.Integration.Tests/  Consumer → Repository against Testcontainers Postgres
  Ecommerce.Payments.Component.Tests/    Full flow against Testcontainers Postgres + Kafka
docs/
  adr/                                Architecture Decision Records
  outbox.md                           Outbox: states, settings, ordering, re-queue, upgrade
  consumed-dead-letters.md            Inbound dead letters: what lands there, listing, replay
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
| `Kafka:ConsumerMaxAttempts` | Failed attempts before an inbound message is stored in `consumed_dead_letters` and committed (default `5`, [ADR 0005](docs/adr/0005-inbound-dead-letter.md)) |
| `PaymentPolicy:MaxAmountThreshold` | Amount above which a payment fails instead of processing |
| `Persistence:OrderLockTimeoutMs` | Maximum wait for the per-order lock before the attempt fails and is retried (default `5000`, [ADR 0004](docs/adr/0004-per-order-serialisation.md)) |
| `OutboxRelay:PollIntervalMs` | Time between outbox relay cycles (default `250`) |
| `OutboxRelay:BatchSize` | Maximum entries published per cycle (default `100`) |
| `OutboxRelay:BaseDelayMs` / `OutboxRelay:MaxDelayMs` | Exponential backoff base and cap after a failed publish (defaults `5000` / `600000`) |
| `OutboxRelay:MaxRetries` | Failed attempts before an entry is `Exhausted` (default `8`) |

Details: [docs/outbox.md](docs/outbox.md).

## Workflow

This repository follows the [Spec-Kit](.specify/) workflow: each user story is specified,
planned, broken into tasks, and implemented under `specs/NNN-feature-name/` before merging to
`main`. See `.specify/memory/constitution.md` for the non-negotiable architectural rules,
[`docs/adr/`](docs/adr/README.md) for architectural decisions, and `CLAUDE.md` for a condensed
guide.
