# Implementation Plan: Process Payment from Order Placed Event

**Branch**: `feature/US-01-process-payment` | **Date**: 2026-07-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-process-payment/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Build the first vertical slice of the payments service: consume `OrderPlaced` from
`orders.order-placed`, run it through the `Payment` aggregate (`PENDING → PROCESSED`), persist the
result to PostgreSQL, and publish `PaymentProcessed` to `payments.payment-processed` — strictly in
that order, idempotently, per Constitution Principles I and II. This is a greenfield service: no
source code exists yet, so this plan also establishes the initial solution/project layout.

## Technical Context

**Language/Version**: C# 14 / .NET 10 (Worker Service, no HTTP surface)

**Primary Dependencies**: `Confluent.Kafka` (consumer + producer), Entity Framework Core with
`Npgsql.EntityFrameworkCore.PostgreSQL`, `Microsoft.Extensions.Hosting` (BackgroundService)

**Storage**: PostgreSQL (via EF Core) — `Payment` table plus a dead-letter table for
post-save publish failures

**Testing**: xUnit + Moq (unit), `Testcontainers.PostgreSql` (integration), `Testcontainers.Kafka`
+ `Testcontainers.PostgreSql` (component)

**Target Platform**: Linux containers (Docker), orchestrated via docker-compose for local/dev

**Project Type**: Single background worker service (event-driven consumer + producer, no
inbound API)

**Performance Goals**: No SLA specified by the business, and none is assumed here — this story is
not load-tested and carries no performance validation task. If a concrete throughput/latency
target emerges later, capture it as an explicit Success Criterion in spec.md first, then add a
corresponding load-test task, rather than inventing a number in this plan.

**Constraints**: At-least-once Kafka delivery (Principle VI) with mandatory idempotent
processing (Principle II); PostgreSQL save MUST complete before any Kafka publish is attempted;
`dynamic`/`object` banned for any data on the Consumer→Service→Domain→Publisher path
(Principle III); offset commit only after full success or confirmed dead-letter handoff.

**Scale/Scope**: Single bounded context (Payments), one consumed topic (`orders.order-placed`),
one published topic (`payments.payment-processed`) for this story, one aggregate (`Payment`).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. DDD & Layered Architecture | Design uses Consumer → Service → Domain → Repository → Publisher with no layer-skipping or merged concerns | PASS |
| II. Event-Driven Write Flow Integrity | DB save strictly precedes Kafka publish; failure paths match constitution exactly (no publish on save failure; dead-letter + offset commit on post-save publish failure) | PASS |
| III. Type Safety & Input Validation | All integration events and DTOs are dedicated typed classes; `PaymentStatus` enum used; nullable reference types enabled | PASS |
| IV. Test Coverage | Unit tests planned for every Domain method and Service handler (happy path + edge case) | PASS |
| V. Testing Strategy | Unit (xUnit/Moq), Integration (testcontainers PostgreSQL), Component (testcontainers Kafka+PostgreSQL) all scoped in Test Plan | PASS |
| VI. Kafka Consumption & Event Publishing | Topics, payload fields (`eventId`, `occurredAt`, `aggregateId`, `version`, + domain fields), and dead-letter behavior match constitution | PASS |
| VII. Branching Strategy | Already developed on `feature/US-01-process-payment`, branched from `main`. Note: the constitution's own worked examples number "process-payment" as US-02 and "handle-payment-failure" as US-03; this repository uses US-01 for process-payment instead, and `Payment.Fail()`/`PaymentFailed`/`payments.payment-failed` are deferred to that future failure-handling story, not built here. Keep this numbering consistent for any future stories in this repo. | PASS |
| VIII. Build & Code Quality Integrity | `dotnet build` / `dotnet format --verify-no-changes` / `dotnet test` will gate every task in the implementation phase | PASS (enforced during `/speckit-implement`, not this planning step) |

No violations identified. Complexity Tracking table is not needed.

## Project Structure

### Documentation (this feature)

```text
specs/001-process-payment/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

This is a greenfield repository — the solution and all projects below are created as part of
implementing this story.

```text
Ecommerce.Payments.sln

src/
├── Ecommerce.Payments.Consumer/          # Consumer layer
│   ├── Program.cs                        # Host builder, DI wiring
│   ├── Consumers/
│   │   └── OrderPlacedConsumer.cs        # BackgroundService; Kafka subscribe, deserialize, delegate, offset commit
│   └── appsettings.json
│
├── Ecommerce.Payments.Service/           # Service layer
│   ├── Payments/
│   │   ├── ProcessPaymentHandler.cs      # Orchestrates Domain -> Repository -> Publisher
│   │   ├── IPaymentRepository.cs
│   │   └── IPaymentEventPublisher.cs
│   └── IntegrationEvents/
│       └── OrderPlacedEvent.cs           # Typed inbound DTO
│
├── Ecommerce.Payments.Domain/             # Domain layer (no infra dependencies)
│   ├── Payments/
│   │   ├── Payment.cs                    # Aggregate root: Create(), Process(), Fail(reason)
│   │   ├── PaymentStatus.cs              # Enum: Pending, Processed, Failed
│   │   ├── PaymentProcessed.cs           # Domain event
│   │   └── InvalidPaymentTransitionException.cs
│   └── Payments/PaymentDomainEvent.cs (base)
│
└── Ecommerce.Payments.Infrastructure/     # Repository + Publisher layers
    ├── Persistence/
    │   ├── PaymentsDbContext.cs
    │   ├── PaymentRepository.cs          # Implements IPaymentRepository; enforces idempotency via unique constraint
    │   ├── Entities/PaymentEntity.cs
    │   ├── Entities/PaymentDeadLetterEntity.cs
    │   └── Migrations/
    └── Messaging/
        ├── PaymentEventPublisher.cs      # Implements IPaymentEventPublisher (Confluent.Kafka producer)
        └── IntegrationEvents/PaymentProcessedEvent.cs  # Typed outbound DTO

tests/
├── Ecommerce.Payments.Domain.Tests/          # Unit — pure, no infra
├── Ecommerce.Payments.Service.Tests/         # Unit — Moq for IPaymentRepository/IPaymentEventPublisher
├── Ecommerce.Payments.Integration.Tests/     # Integration — testcontainers PostgreSQL
└── Ecommerce.Payments.Component.Tests/       # Component — testcontainers Kafka + PostgreSQL
```

**Structure Decision**: Single-solution, multi-project Clean Architecture layout mirroring the
constitution's five layers 1:1 (`Consumer` = Consumer, `Service` = Service, `Domain` = Domain,
`Infrastructure/Persistence` = Repository, `Infrastructure/Messaging` = Publisher). One
`Infrastructure` project houses both Repository and Publisher since both are outward-facing
adapters with no interdependency risk; they are kept in separate folders/namespaces and never
referenced from Domain. This is the "Option 1: Single project" shape from the template, adapted
to the layered folder names the constitution already names explicitly.

## Complexity Tracking

*No violations — table intentionally left empty.*
