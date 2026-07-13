# Implementation Plan: Handle Payment Failure

**Branch**: `feature/US-02-handle-payment-failure` | **Date**: 2026-07-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/002-handle-payment-failure/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Extend the already-shipped US-01 flow with a failure branch: when a `Payment`'s amount exceeds
a configurable maximum threshold, the `Payment` aggregate transitions to `Failed` (with a
reason) instead of `Processed`, and `PaymentFailed` is published to `payments.payment-failed`
instead of `PaymentProcessed` to `payments.payment-processed` — under the exact same
save-before-publish, idempotent, retry-on-technical-failure guarantees US-01 already
established. This is additive to the existing codebase, not a rewrite: `Payment.Process()`,
`ProcessPaymentHandler`'s orchestration shape, `IPaymentRepository`, and the Consumer are all
extended, not replaced.

## Technical Context

**Language/Version**: C# 14 / .NET 10 (unchanged from US-01)

**Primary Dependencies**: Same as US-01 — `Confluent.Kafka`, EF Core + `Npgsql.EntityFrameworkCore.PostgreSQL`,
`Polly`, `Microsoft.Extensions.Hosting`/`Options`. No new package dependencies required.

**Storage**: PostgreSQL (via EF Core) — extends the existing `payments` table with two new
nullable columns (`FailureReason`, `FailedAt`); no new tables.

**Testing**: xUnit + Moq (unit), `Testcontainers.PostgreSql` (integration), `Testcontainers.Kafka`
+ `Testcontainers.PostgreSql` (component) — same tiers as US-01.

**Target Platform**: Same running process as US-01 — this is not a new service, it's new
behavior inside the existing `Ecommerce.Payments.Consumer` host.

**Project Type**: Extension of the existing single background worker service (US-01's solution
layout is reused as-is; no new projects).

**Performance Goals**: No SLA specified; unchanged from US-01 — not load-tested, no new
performance-sensitive path introduced (one extra `if` comparison in Domain).

**Constraints**: Must not change `Payment.Process()`'s existing signature/behavior (US-01 code
and its 21 passing unit tests depend on it meaning "unconditionally succeed"); the amount-vs-
threshold business rule MUST live in Domain, not Service (Constitution Principle I: "A Service
method that branches on payment amounts... is a violation"); a business failure MUST NOT be
logged/alerted as a system error (spec FR-008).

**Scale/Scope**: Same bounded context (Payments), same consumed topic (`orders.order-placed`),
one new published topic (`payments.payment-failed`), same aggregate (`Payment`, extended).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. DDD & Layered Architecture | The amount-threshold business rule is evaluated entirely inside a new `Payment.Evaluate()` Domain method; Service only routes an already-decided domain event by type — it never inspects `Amount` itself | PASS |
| II. Event-Driven Write Flow Integrity | Save-before-publish, idempotency, and retry ordering are unchanged and apply identically to the failure branch (same `SavePipeline`, same `ExistsByOrderIdAsync` gate) | PASS |
| III. Type Safety & Input Validation | `PaymentFailedEvent` is a dedicated typed DTO, same pattern as `PaymentProcessedEvent`; `PaymentStatus.Failed` already exists as an enum member (added defensively in US-01) | PASS |
| IV. Test Coverage | Unit tests planned for `Payment.Fail()` (happy path + invalid transition) and `Payment.Evaluate()` (both branches); Service handler tests verify correct publisher routing and that a business failure never logs as an error | PASS |
| V. Testing Strategy | Unit, Integration (Consumer→Repository persisting `Failed` status + reason), Component (full flow publishing to `payments.payment-failed`) all scoped in Test Plan | PASS |
| VI. Kafka Consumption & Event Publishing | New topic `payments.payment-failed` follows the `payments.<event-name>` convention; payload carries `eventId`/`occurredAt`/`aggregateId`/`version` plus `orderId`/`reason`/`failedAt` per Constitution | PASS |
| VII. Branching Strategy | Developed on `feature/US-02-handle-payment-failure`, branched from `main` (which already has US-01 merged) | PASS |
| VIII. Build & Code Quality Integrity | `dotnet build`/`format`/`test` gate applies per task during `/speckit-implement`, same as US-01 | PASS (enforced during implementation) |

No violations identified. Complexity Tracking table is not needed.

## Project Structure

### Documentation (this feature)

```text
specs/002-handle-payment-failure/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

No new projects — this feature extends the existing solution from US-01 in place. Files marked
**(new)** are added; all others are existing US-01 files being extended.

```text
src/
├── Ecommerce.Payments.Consumer/
│   ├── Consumers/OrderPlacedConsumer.cs      # Unchanged — dispatch doesn't inspect outcome type
│   ├── Program.cs                            # Extended: bind PaymentPolicyOptions, register new publisher method's config
│   └── appsettings.json                      # Extended: Kafka:PaymentFailedTopic, PaymentPolicy:MaxAmountThreshold
│
├── Ecommerce.Payments.Service/
│   └── Payments/
│       ├── ProcessPaymentHandler.cs          # Extended: calls Payment.Evaluate(), routes result by type
│       ├── IPaymentEventPublisher.cs         # Extended: + PublishFailedAsync(PaymentFailed, ...)
│       └── PaymentPolicyOptions.cs           # (new) MaxAmountThreshold, bound from config
│
├── Ecommerce.Payments.Domain/
│   └── Payments/
│       ├── Payment.cs                        # Extended: + FailureReason, FailedAt, Fail(reason), Evaluate(maxAmountThreshold)
│       └── PaymentFailed.cs                  # (new) Domain event, mirrors PaymentProcessed.cs
│
└── Ecommerce.Payments.Infrastructure/
    ├── Persistence/
    │   ├── Entities/PaymentEntity.cs         # Extended: + FailureReason, FailedAt columns
    │   ├── PaymentsDbContext.cs               # Extended: column mapping for the two new fields
    │   ├── PaymentRepository.cs               # Extended: maps FailureReason/FailedAt on save
    │   └── Migrations/                        # (new) AddPaymentFailureFields migration
    └── Messaging/
        ├── KafkaOptions.cs                    # Extended: + PaymentFailedTopic
        ├── PaymentEventPublisher.cs           # Extended: + PublishFailedAsync (same dead-letter fallback as PublishAsync)
        └── IntegrationEvents/PaymentFailedEvent.cs  # (new) Typed outbound DTO

tests/
├── Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs           # Extended: Fail()/Evaluate() cases
├── Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs  # Extended: routing + no-error-log cases
├── Ecommerce.Payments.Integration.Tests/                              # (new) OrderPlacedToFailedPaymentTests.cs
└── Ecommerce.Payments.Component.Tests/                                # (new) ProcessPaymentFailureFlowTests.cs
```

**Structure Decision**: Pure extension of US-01's existing five-project layout — no new
projects, no structural changes. Every file above either already exists (and is being extended
in place) or is a small, focused new file that fits the pattern US-01 already established (e.g.
`PaymentFailed.cs` mirrors `PaymentProcessed.cs`; `PaymentFailedEvent.cs` mirrors
`PaymentProcessedEvent.cs`). This keeps the two stories visually and structurally symmetric,
which matters since they're two branches of the same underlying decision.

## Complexity Tracking

*No violations — table intentionally left empty.*
