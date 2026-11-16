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

- Messages in `consumed_dead_letters` are not replayed automatically. A parked `OrderCancelled`
  leaves its payment `Processed` until it is replayed. Rows are purged after 90 days
  ([docs/retention.md](docs/retention.md)).
- The consumer keeps its attempt count in memory, so after a restart the count for a failing
  message starts again.
- An `Exhausted` outbox entry holds back later events for the same payment until it is
  re-queued ([docs/outbox.md](docs/outbox.md)).
- Upgrading to US-07: stop the service, run the migration (`payment_dead_letters` becomes
  `payment_outbox`), then start it. The `DeadLetterRetry` settings section is now `OutboxRelay`.
- Cancellation markers are purged after `Retention:CancellationMarkersDays` (180 days). If an
  `OrderPlaced` arrives later than that behind its `OrderCancelled`, the cancelled order is
  charged ([docs/retention.md](docs/retention.md)). Early cancellations that US-04 ignored
  before US-05 shipped are not backfilled.

## Background services

| Hosted service | Responsibility | Status |
|---|---|---|
| `DatabaseMigrator` | Starts first. It applies pending EF Core migrations before any consumer or worker runs. While PostgreSQL is unreachable it retries `DatabaseMigration:MaxAttempts` times (10, 3 s apart). If that fails, or a migration fails, it logs Critical and the process exits with code 1. Set `DatabaseMigration:Enabled=false` to apply migrations some other way. | Implemented |
| `OrderPlacedConsumer` | Consumes `orders.order-placed` and runs the payment write flow. It skips orders that have a cancellation marker (US-05). Messages that are malformed or keep failing go to `consumed_dead_letters` (US-06). | Implemented |
| `OutboxRelayWorker` | Every `OutboxRelay:PollIntervalMs` (250 ms) it publishes due `payment_outbox` entries unchanged (same topic, key and `eventId`), in recording order per payment. It retries failures with exponential backoff and marks an entry `Exhausted` after `MaxRetries`. Entries are claimed with `FOR UPDATE SKIP LOCKED`, so several instances are safe. See [docs/outbox.md](docs/outbox.md) and [ADR 0006](docs/adr/0006-transactional-outbox.md). | Implemented (US-03, US-07) |
| `RetentionWorker` | Shortly after startup and then every `Retention:IntervalMs` (1 h), it purges `Published` outbox entries, inbound dead letters and cancellation markers older than their retention periods (30 / 90 / 180 days by default, `0` = keep forever). It deletes in batches with `FOR UPDATE SKIP LOCKED`, so several instances are safe, and logs the counts. `Pending` and `Exhausted` outbox entries and payments are never purged. See [docs/retention.md](docs/retention.md) and [ADR 0007](docs/adr/0007-data-retention.md). | Implemented (US-09) |
| `OrderCancelledConsumer` | Consumes `orders.order-cancelled`. Refunds the order's `Processed` payment and records `PaymentRefunded` in the outbox with the refund (US-07). Failed, Pending or already Refunded payments are ignored with an Information log. With no payment, it records a cancellation marker (US-05). Messages that are malformed or keep failing go to `consumed_dead_letters` (US-06). | Implemented (US-04, US-05, US-06) |

## Project layout

```
src/
  Ecommerce.Payments.Consumer/        Worker Service host, Kafka consumers
  Ecommerce.Payments.Service/         Application/handler layer
  Ecommerce.Payments.Domain/          Payment aggregate, domain events, business rules
  Ecommerce.Payments.Infrastructure/  EF Core repositories, outbox and its relay, retention purge, Kafka publisher, migrations
tests/
  Ecommerce.Payments.Domain.Tests/          Unit tests, no infrastructure
  Ecommerce.Payments.Service.Tests/         Unit tests with mocked Repository/Publisher
  Ecommerce.Payments.Infrastructure.Tests/  Unit tests for dead-letter retry policy/processor/worker (fake clock)
  Ecommerce.Payments.Integration.Tests/  Consumer → Repository against Testcontainers Postgres
  Ecommerce.Payments.Component.Tests/    Full flow against Testcontainers Postgres + Kafka
docs/
  adr/                                Architecture Decision Records
  outbox.md                           Outbox: states, settings, ordering, re-queue, upgrade
  retention.md                        Data retention: what is purged and when, settings, marker trade-off
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

The worker applies pending EF Core migrations itself when it starts (`DatabaseMigrator`), so
no manual step is needed. To apply them by hand instead (for example, with
`DatabaseMigration:Enabled=false`):

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

## Docker

### Build

```powershell
docker build -t ecommerce-payments .
```

The image is built in two stages:
- **Build stage:** the .NET 10 SDK restores and publishes the worker for the target
  architecture only (`amd64` or `arm64`, from Docker's `TARGETARCH`). The image therefore
  carries a single librdkafka native library.
- **Runtime stage:** `mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled`, the smallest
  glibc-based .NET runtime, which librdkafka needs. It has no shell or package manager and runs
  as the non-root `app` user (UID 1654).

`.dockerignore` keeps tests, docs, specs and tooling out of the build context.

### Run

The container reads `appsettings.json` and then environment variables, which override it. Use
`__` as the section separator, for example `Kafka__BootstrapServers` for
`Kafka:BootstrapServers`. On startup it applies pending migrations before consuming anything.

Against the shared `ecommerce-infra` stack, join its network and use the service names. Kafka's
in-network listener is `kafka:29092`; `localhost:9092` only works from the host.

```powershell
docker run -d --name ecommerce-payments `
  --network ecommerce_default `
  --stop-timeout 30 `
  -e ConnectionStrings__Payments="Host=postgres-payments;Port=5432;Database=payments;Username=payments_user;Password=payments_pass" `
  -e Kafka__BootstrapServers=kafka:29092 `
  -e Kafka__ConsumerGroupId=payments-service `
  ecommerce-payments

docker logs -f ecommerce-payments
```

- `--stop-timeout 30`: on `docker stop` the worker leaves its Kafka consumer group and finishes
  the current message. This can take longer than Docker's default 10 s.
- Exit code `1` means startup migration failed. The Critical log line explains why: the database
  was unreachable after every attempt, or a migration failed.
- A `Cannot load library libgssapi_krb5.so.2` line at startup is harmless. Npgsql probes for
  Kerberos, which the chiseled image doesn't ship and this service doesn't use.

### Environment variables

| Variable | Default (`appsettings.json`) | Purpose |
|---|---|---|
| `ConnectionStrings__Payments` | `Host=localhost;Port=5434;…` | PostgreSQL connection string (**required** in a container) |
| `Kafka__BootstrapServers` | `localhost:9092` | Kafka brokers (**required** in a container) |
| `Kafka__ConsumerGroupId` | `payments-service` | Consumer group shared by all instances |
| `Kafka__ConsumerMaxAttempts`, `Kafka__ConsumerRetryBaseDelayMs`, `Kafka__ConsumerRetryMaxDelayMs` | `5`, `1000`, `60000` | In-place consumer retries before an inbound message is dead-lettered |
| `Kafka__*Topic` | see [Configuration reference](#configuration-reference) | Topic names |
| `DatabaseMigration__Enabled` | `true` | Apply pending migrations at startup |
| `DatabaseMigration__MaxAttempts`, `DatabaseMigration__RetryDelayMs` | `10`, `3000` | Wait for PostgreSQL to become reachable, then fail fast |
| `OutboxRelay__PollIntervalMs`, `OutboxRelay__BatchSize`, `OutboxRelay__BaseDelayMs`, `OutboxRelay__MaxDelayMs`, `OutboxRelay__MaxRetries` | `250`, `100`, `5000`, `600000`, `8` | Outbox relay (replaces the old `DeadLetterRetry__*` settings since US-07) |
| `Retention__IntervalMs`, `Retention__BatchSize`, `Retention__OutboxPublishedDays`, `Retention__ConsumedDeadLettersDays`, `Retention__CancellationMarkersDays` | `3600000`, `1000`, `30`, `90`, `180` | Data retention |
| `Persistence__OrderLockTimeoutMs` | `5000` | Per-order lock wait |
| `PaymentPolicy__MaxAmountThreshold` | `10000.00` | Amount above which a payment fails |
| `Logging__LogLevel__Default` | `Information` | Log level, e.g. `Logging__LogLevel__Microsoft.EntityFrameworkCore=Warning` silences SQL logging |
| `DOTNET_ENVIRONMENT` | `Production` | Host environment (loads `appsettings.{Environment}.json` if present) |

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
| `DatabaseMigration:Enabled` | Apply pending EF Core migrations at startup, before anything consumes (default `true`) |
| `DatabaseMigration:MaxAttempts` / `DatabaseMigration:RetryDelayMs` | Attempts while PostgreSQL is unreachable, and the wait between them, before startup fails with exit code 1 (defaults `10` / `3000`) |
| `Persistence:OrderLockTimeoutMs` | Maximum wait for the per-order lock before the attempt fails and is retried (default `5000`, [ADR 0004](docs/adr/0004-per-order-serialisation.md)) |
| `OutboxRelay:PollIntervalMs` | Time between outbox relay cycles (default `250`) |
| `OutboxRelay:BatchSize` | Maximum entries published per cycle (default `100`) |
| `OutboxRelay:BaseDelayMs` / `OutboxRelay:MaxDelayMs` | Exponential backoff base and cap after a failed publish (defaults `5000` / `600000`) |
| `OutboxRelay:MaxRetries` | Failed attempts before an entry is `Exhausted` (default `8`) |
| `Retention:IntervalMs` / `Retention:BatchSize` | Time between retention purges and rows per delete statement (defaults `3600000` / `1000`) |
| `Retention:OutboxPublishedDays` / `Retention:ConsumedDeadLettersDays` / `Retention:CancellationMarkersDays` | Days published outbox entries / inbound dead letters / cancellation markers are kept before purging; `0` keeps forever (defaults `30` / `90` / `180`, [docs/retention.md](docs/retention.md)) |

Details: [docs/outbox.md](docs/outbox.md).

## Workflow

This repository follows the [Spec-Kit](.specify/) workflow: each user story is specified,
planned, broken into tasks, and implemented under `specs/NNN-feature-name/` before merging to
`main`. See `.specify/memory/constitution.md` for the non-negotiable architectural rules,
[`docs/adr/`](docs/adr/README.md) for architectural decisions, and `CLAUDE.md` for a condensed
guide.
