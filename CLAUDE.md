# CLAUDE.md

Guidance for Claude Code in this repository. The source of truth for every rule here is
`.specify/memory/constitution.md` (v1.3.0); if this file and the constitution disagree, the
constitution wins. Architectural decisions live in `docs/adr/`.

## Stack

- .NET 10 / C# **Worker Service** — a pure Kafka consumer/producer, no HTTP API.
- Kafka via `Confluent.Kafka`; EF Core + PostgreSQL (Npgsql).
- Tests: xUnit, Moq, `FakeTimeProvider`, testcontainers-dotnet. Unit tests: Domain.Tests,
  Service.Tests, Infrastructure.Tests (outbox relay); Integration.Tests (PostgreSQL
  container); Component.Tests (PostgreSQL + Kafka containers).
- Local infrastructure comes from the sibling `ecommerce-infra` repo (docker compose):
  Kafka on `localhost:9092`, Postgres for payments on `localhost:5434`
  (database `payments`, user `payments_user`, password `payments_pass`).
- Sibling service: `ecommerce-orders` (the other side of the saga).

## Commands

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test                       # Integration/Component tests need Docker running

dotnet tool restore               # installs the pinned dotnet-ef
dotnet ef migrations add <Name> --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure
dotnet ef database update      --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure
dotnet ef migrations remove    --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure

dotnet run --project src/Ecommerce.Payments.Consumer
docker build -t ecommerce-payments .
```

The design-time `PaymentsDbContextFactory` uses `PAYMENTS_CONNECTION_STRING` if set, otherwise
the `ecommerce-infra` defaults above.

## Layering (non-negotiable)

**Consumer → Service → Domain → Repository → Publisher** — one way, no skipping, no merging.

| Layer | Project | Rules |
|---|---|---|
| Consumer | `Ecommerce.Payments.Consumer` | Hosted services. Deserialize into typed DTOs, validate the envelope, delegate. No business logic. Commit offsets only after the Service finishes. Catch every exception — one bad message never crashes the host. |
| Service | `Ecommerce.Payments.Service` | Orchestrates Domain → Repository (state change + outbox entry in one transaction). Never publishes. No business rules (no branching on amounts/statuses). |
| Domain | `Ecommerce.Payments.Domain` | `Payment` aggregate owns every rule and transition (`Process`, `Fail`, `Refund`). Plain C#: no EF Core, Kafka or hosting types. No public status setters. |
| Repository | `Ecommerce.Payments.Infrastructure/Persistence` | EF Core behind `IPaymentRepository`. Never leak EF types into Domain. |
| Publisher | `Ecommerce.Payments.Infrastructure/Messaging` | Kafka producer behind `IRawEventPublisher`. Called only by the outbox relay, never by the Service (US-07). |

Also: no `dynamic`/`object` for event data (enforced by `BannedSymbols.txt`), nullable enabled,
`PaymentStatus` enum instead of strings, domain exceptions instead of `null`/`bool` returns.

## Write flow (order matters): transactional outbox

This follows constitution v1.3.0 Principle II, US-07 and
[ADR 0006](docs/adr/0006-transactional-outbox.md).

1. Consume and deserialize the event.
2. Domain executes the transition and raises the domain event.
3. **In one transaction**, save the state change **and** record the event in `payment_outbox`.
   Use `IPaymentOutbox.AddProcessedAsync`, `AddFailedAsync` or `AddRefundedAsync` inside the
   order transaction, after the save and before `CommitAsync`.
   - If the transaction fails, nothing is saved and nothing is published. Do not commit the
     offset; retry.
4. Commit the offset once the transaction has committed. Do not wait for Kafka.
5. The **outbox relay** publishes the entry unchanged: same `eventId`, topic and key.

Handlers never publish. Nothing except the relay calls `IRawEventPublisher`, and there is no
`IPaymentEventPublisher`. Publishing anything other than a committed outbox entry is a critical
violation.

**Outbox relay**: `OutboxRelayWorker` and `OutboxRelayProcessor`, in
`Ecommerce.Payments.Infrastructure/Outbox`, registered in the Consumer host. Details are in
`docs/outbox.md`.
- It polls every `OutboxRelay:PollIntervalMs` (250 ms). Each entry is claimed in its own
  transaction with `FOR UPDATE SKIP LOCKED`, in `Sequence` order.
- **Never ahead of an unpublished earlier entry for the same payment.** An `Exhausted` entry
  blocks later events for that payment until it is re-queued.
- On success it marks the entry `Published`. On failure it backs off exponentially, and marks
  the entry `Exhausted` after `MaxRetries`.

Rules:
- There is a single outbound store, `payment_outbox`. Do not add another dead-letter path for
  publishes.
- The relay persists through `IPaymentOutboxRepository`. Handlers write through
  `IPaymentOutbox` (`AddPaymentOutbox()`). Tests that build handlers need both of these, plus
  `AddOrderPersistence()`.
- Integration tests assert recorded events with `OutboxReader`. Component tests host the relay
  through `AddOutboxRelay()` from `OutboxTestServices`.
- All time decisions use the injected `TimeProvider`; tests use `FakeTimeProvider`.
  - PostgreSQL stores microseconds, so tests that compare stored timestamps should start fake
    clocks on whole seconds.
  - Tests that make entries due must use the host clock, not the database's `now()`.

Startup must never depend on Kafka being reachable: connectivity problems are logged and retried
in the background.

**Per-order unit of work** (US-05, [ADR 0004](docs/adr/0004-per-order-serialisation.md)):
- Every handler that reads or changes an order's state runs its read-decide-write step inside
  `await using var tx = await IOrderUnitOfWork.BeginAsync(orderId)`. That transaction first takes
  `pg_advisory_xact_lock` for the order; the wait is bounded by `Persistence:OrderLockTimeoutMs`.
- Commit with `tx.CommitAsync`; disposing without a commit rolls back and clears the change
  tracker.
- Wrap the **whole** unit (begin → reads → Domain → save → commit) in
  `PersistenceRetry.Pipeline`, not single statements.
- Record events in the outbox inside the transaction; never publish from a handler (the relay does).
- Register with `services.AddOrderPersistence()`. Tests that build handlers need it too.

## Idempotency

- Kafka is at-least-once; every handler must be safe to run twice.
- The **`aggregateId` of the order event (the order id) is the idempotency key**: at most one
  `Payment` per order, and a redelivered `OrderPlaced` never creates a second payment or a second
  published event.
- Consumers also deduplicate redelivered messages by `eventId`. For refunds: the cancelling
  `eventId` is stored on the payment as `RefundSourceEventId`; a redelivery with that id is
  ignored before Domain, and a re-published cancellation (new `eventId`) is ignored because the
  payment is already `Refunded`. `SaveRefundAsync` is conditional (only if still Processed) so
  concurrent deliveries cannot refund twice. (`OrderPlacedConsumer` still dedupes by order id —
  pre-existing.)
- New consumers derive from `KafkaConsumerBase`:
  - It commits after success, retries the same message in place with backoff, and never commits
    past a failure.
  - After `Kafka:ConsumerMaxAttempts` (default 5), it stores the message in
    `consumed_dead_letters` and commits only after that store succeeds. If the store fails,
    nothing is committed and the message is retried
    ([ADR 0005](docs/adr/0005-inbound-dead-letter.md), US-06).
  - In `HandleMessageAsync`, throw `InvalidMessageException` for a malformed envelope; it is
    stored with `Attempts = 1` and never retried. Let every other failure propagate. Do not
    return `true` for bad input.
  - `consumed_dead_letters` holds inbound messages. `payment_outbox` holds every outbound
    event and is published by the relay. Do not mix them. Inbound replay is manual
    (`docs/consumed-dead-letters.md`).

## Event contracts

| Direction | Topic | Event | Notes |
|---|---|---|---|
| Consumes | `orders.order-placed` | `OrderPlaced` | `eventId`, `occurredAt`, `aggregateId` (= order id), `version`, `customerId`, `totalAmount`. No `orderId`/`currency` field (currency defaults to USD). |
| Consumes | `orders.order-cancelled` | `OrderCancelled` | `eventId`, `occurredAt`, `aggregateId` (= order id), `version`; optional `customerId`, `reason`. Refunds a `Processed` payment (US-04). |
| Publishes | `payments.payment-processed` | `PaymentProcessed` | + `orderId`, `amount`, `currency`, `processedAt`. |
| Publishes | `payments.payment-failed` | `PaymentFailed` | + `orderId`, `reason`, `failedAt`. |
| Publishes | `payments.payment-refunded` | `PaymentRefunded` | + `orderId`, `amount`, `currency`, `refundedAt`; key = payment id. |

- Every published payment event carries `eventId`, `occurredAt`, `aggregateId` = the **payment**
  id, `version`, and a separate `orderId` = the order id. Downstream consumers must use
  `orderId` to identify the order.
- Refunds only from `Processed`. A refund request for a `Failed` or already `Refunded` payment
  is logged and ignored (offset committed) — it never throws.
- A cancellation for an order with **no payment** records a cancellation marker (one per order,
  unique constraint). An `OrderPlaced` for a marked order creates no payment and publishes
  nothing. Marker and payment creation for the same order must be mutually exclusive under
  concurrency (constitution v1.2.0, Principle VI "Cancellation markers").
- JSON payloads; breaking changes need a new topic version (e.g. `payments.payment-processed.v2`).
- Event classes carry XML doc comments on every field.

## Branching and commits

- Never commit to `main`. Branch from an up-to-date `main` with a clean working tree:
  `feature/US-XX-short-description`, `fix/US-XX-short-description`, `chore/short-description`.
- One branch per user story; for `/speckit.specify` the branch must exist before any spec file
  is written.
- Conventional Commits, scoped by story where applicable: `feat(US-03): ...`,
  `test(US-03): ...`, `docs: ...`, `chore: ...`.
- PRs reference the story: "Implements US-03: ...". Delete branches after merge.
- New architectural decisions or duplicate-capability dependencies need an ADR in `docs/adr/`.
- New background services are documented in the root `README.md`.

## Quality gate (after every task)

Run in order; all three must pass before a task is marked complete or anything is committed:

1. `dotnet build` — zero errors.
2. `dotnet format --verify-no-changes` — no formatting drift.
3. `dotnet test` — all tests green (Docker must be running for Testcontainers).

New domain methods and handlers need unit tests for the happy path and at least one error/edge
case; behaviour-describing test names (`Should_Publish_PaymentProcessed_After_Successful_Save`).
