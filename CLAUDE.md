# CLAUDE.md

Guidance for Claude Code in this repository. The source of truth for every rule here is
`.specify/memory/constitution.md` (v1.1.0); if this file and the constitution disagree, the
constitution wins. Architectural decisions live in `docs/adr/`.

## Stack

- .NET 10 / C# **Worker Service** — a pure Kafka consumer/producer, no HTTP API.
- Kafka via `Confluent.Kafka`; EF Core + PostgreSQL (Npgsql).
- Tests: xUnit, Moq, `FakeTimeProvider`, testcontainers-dotnet. Unit tests: Domain.Tests,
  Service.Tests, Infrastructure.Tests (dead-letter retry); Integration.Tests (PostgreSQL
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
| Service | `Ecommerce.Payments.Service` | Orchestrates Domain → Repository → Publisher. No business rules (no branching on amounts/statuses). |
| Domain | `Ecommerce.Payments.Domain` | `Payment` aggregate owns every rule and transition (`Process`, `Fail`, `Refund`). Plain C#: no EF Core, Kafka or hosting types. No public status setters. |
| Repository | `Ecommerce.Payments.Infrastructure/Persistence` | EF Core behind `IPaymentRepository`. Never leak EF types into Domain. |
| Publisher | `Ecommerce.Payments.Infrastructure/Messaging` | Kafka producer behind `IPaymentEventPublisher`. Called only by the Service. |

Also: no `dynamic`/`object` for event data (enforced by `BannedSymbols.txt`), nullable enabled,
`PaymentStatus` enum instead of strings, domain exceptions instead of `null`/`bool` returns.

## Write flow (order matters)

1. Consume and deserialize the event.
2. Domain executes the transition and raises the domain event.
3. **Save to PostgreSQL first.** If the save fails: publish nothing, do not commit the offset,
   retry.
4. **Then publish to Kafka.** If the publish fails: log, write the event to
   `payment_dead_letters`, and still commit the offset (the DB is the source of truth).
5. Commit the offset.

Publishing before a confirmed save is a critical violation.

**Dead-letter retry** (US-03): `DeadLetterRetryWorker` (`Ecommerce.Payments.Infrastructure/DeadLetters`,
registered in the Consumer host) claims due `PendingRetry` rows one per transaction with
`FOR UPDATE SKIP LOCKED`, republishes the stored payload unchanged (same topic, key, `eventId`)
and marks rows `Resolved`, or reschedules them with exponential backoff until `Exhausted` after
`MaxRetries`. Settings: `DeadLetterRetry` section. Details: `docs/dead-letter-retry.md`,
`docs/adr/0003-dead-letter-retry.md`. Rules:
- Retry paths publish through `IRawEventPublisher.PublishRawAsync` (throws, never
  dead-letters) — never through `IPaymentEventPublisher`, which creates a new event id and a new
  dead-letter row on failure.
- All dead-letter persistence goes through `IPaymentDeadLetterRepository`.
- All time decisions use the injected `TimeProvider` (tests use `FakeTimeProvider`). PostgreSQL
  stores microseconds, so tests comparing stored timestamps should start fake clocks on whole
  seconds.

Startup must never depend on Kafka being reachable: connectivity problems are logged and retried
in the background.

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
- New consumers derive from `KafkaConsumerBase` (commit after success, retry the same message
  in place with backoff, never commit past a failure).

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
- Refunds only from `Processed`. A refund request for a `Failed`, already `Refunded` or unknown
  payment is logged and ignored (offset committed) — it never throws.
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
