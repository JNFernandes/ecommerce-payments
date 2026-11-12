# Implementation Plan: Dead-Letter Retry

**Branch**: `feature/US-03-dead-letter-retry` | **Date**: 2026-11-12 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/003-dead-letter-retry/spec.md`

## Summary

Payment events that failed to publish after a successful save sit in `payment_dead_letters`
forever today. This feature adds retry bookkeeping to that table (one EF Core migration), a
`DeadLetterRetryWorker : BackgroundService` in `Ecommerce.Payments.Infrastructure` that on a
configurable interval claims due rows one at a time with `SELECT … FOR UPDATE SKIP LOCKED`,
republishes the stored payload unchanged (same topic, key and `eventId`) through a new raw
publish method on the existing `PaymentEventPublisher`, and marks the row `Resolved` or
reschedules it with capped exponential backoff until it becomes `Exhausted`. All time decisions
use `TimeProvider`. The existing dead-letter write is extracted from the publisher into a
`PaymentDeadLetterRepository` that both paths share.

**Inbound dead letters**: the Consumer does **not** store failed inbound messages (it relies on
uncommitted offsets and Kafka redelivery), so inbound retry is **out of scope** (research R1).

## Technical Context

**Language/Version**: C# / .NET 10 (SDK 10.0.303)

**Primary Dependencies**: Confluent.Kafka 2.15.0, EF Core 10.0.4 + Npgsql provider 10.0.2,
Microsoft.Extensions.Hosting(.Abstractions) 10.0.9 (new reference in Infrastructure for
`BackgroundService`), Microsoft.Extensions.TimeProvider.Testing 10.11.0 (tests only)

**Storage**: PostgreSQL (`payments` database), table `payment_dead_letters`

**Testing**: xUnit, Moq, FakeTimeProvider, Testcontainers (PostgreSQL, Kafka)

**Target Platform**: Linux container / Windows dev, .NET Worker Service

**Project Type**: Event-driven background worker (no HTTP surface)

**Performance Goals**: due row republished within ~1 interval (default 30 s); batch of 20 rows
per cycle

**Constraints**: row locks held only for one publish (≤ 10 s producer timeout); startup never
waits on Kafka; clean shutdown via the stopping token; no change to domain logic, write-flow
order or event contracts

**Scale/Scope**: low volume (rows only appear during broker outages); safe with N instances

No open NEEDS CLARIFICATION — all resolved in [research.md](research.md).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. Layering | Domain untouched. Persistence moves into a Repository (`PaymentDeadLetterRepository`); Kafka stays in the Publisher. Worker is an infrastructure host that only uses Repository + Publisher (see Complexity Tracking). EF types do not leak into Domain/Service. | ✅ (justified) |
| II. Write-flow integrity | `ProcessPaymentHandler` order unchanged (save → publish → dead-letter on publish failure → commit). Retry only republishes already-saved outcomes. Idempotency rules unchanged. | ✅ |
| III. Type safety | Typed entity/enum/options; payload stays an opaque `string` already serialized from a typed event — no `dynamic`/`object`/`JsonElement`. Nullable on. | ✅ |
| IV. Test coverage | Policy + processor unit tests (happy + error paths); claim integration tests; component test. Behaviour-describing names. | ✅ |
| V. Testing strategy | Unit (no infra) / Integration (PostgreSQL container) / Component (Kafka + PostgreSQL containers) all present. | ✅ |
| VI. Kafka & events | Dead-letter retry exactly as amended in v1.1.0: statuses, retry count, next retry, last error, exponential backoff, max retries, SKIP LOCKED, original `eventId`. Startup independent of Kafka. No topic/contract changes. | ✅ |
| VII. Branching | `feature/US-03-dead-letter-retry` from up-to-date `main`; Conventional Commits `feat(US-03)`, `test(US-03)`, `docs(US-03)`. | ✅ |
| VIII. Quality gate | `dotnet build` → `dotnet format --verify-no-changes` → `dotnet test` after every task. | ✅ |
| Docs standards | XML docs on all public types; `docs/dead-letter-retry.md`; root README background-services table, Infrastructure/Consumer READMEs, `CLAUDE.md`; references ADR 0003. | ✅ |

**Post-design re-check (after Phase 1)**: unchanged — design artifacts introduce no new topics,
no domain changes, and keep publishing after a confirmed save. ✅

## Design

### Components (all new code in `Ecommerce.Payments.Infrastructure` unless noted)

| Component | Kind | Responsibility |
|---|---|---|
| `Persistence/Entities/PaymentDeadLetterEntity` | **extended** | + `Topic`, `MessageKey`, `Status`, `RetryCount`, `NextRetryAt`, `LastAttemptAt`, `LastError`, `ResolvedAt`. |
| `Persistence/Entities/DeadLetterStatus` | new enum | `PendingRetry`, `Resolved`, `Exhausted`. |
| `Persistence/PaymentsDbContext` | **extended** | String conversion for `Status`, partial due index. |
| `Persistence/Migrations/<ts>_AddDeadLetterRetryBookkeeping` | new | The single migration + backfill SQL (data-model.md). |
| `Persistence/IPaymentDeadLetterRepository`, `PaymentDeadLetterRepository` | new (extracted) | `AddAsync` (the write moved out of the publisher), `ClaimDueAsync(now, limit)` (raw SQL, must run inside a transaction), `SaveChangesAsync`, `BeginTransactionAsync`. |
| `Messaging/IRawEventPublisher` | new interface | `PublishRawAsync(topic, key, payload, ct)` — throws on failure, never dead-letters. |
| `Messaging/PaymentEventPublisher` | **extended** | Implements `IRawEventPublisher`; `PublishAsync`/`PublishFailedAsync` call it and, on failure, dead-letter via the repository with topic + key + `TimeProvider` timestamps. |
| `DeadLetters/DeadLetterRetryOptions` | new | Settings + defaults, section `DeadLetterRetry`. |
| `DeadLetters/DeadLetterRetryPolicy` | new, static/pure | `ComputeDelay(retryCount, options)`; `ApplySuccess(row, now)`; `ApplyFailure(row, error, now, options)` → returns whether exhausted. |
| `DeadLetters/DeadLetterRetryProcessor` | new, scoped | One cycle: up to `BatchSize` × (tx → claim 1 → publish raw → apply policy → save → commit). Logs per contract. Returns rows processed. |
| `DeadLetters/DeadLetterRetryWorker` | new `BackgroundService` | `Task.Yield()`, then loop: create scope → `processor.RunCycleAsync` (catch-all log) → `PeriodicTimer(Interval, TimeProvider)`. Exits on stopping token. |
| `Consumer/Program.cs` (Consumer project) | **extended** | Bind + validate options, `TimeProvider.System`, repository, single publisher instance forwarded to both interfaces, processor, `AddHostedService<DeadLetterRetryWorker>()`. |
| `Consumer/appsettings.json` | **extended** | `DeadLetterRetry` section with defaults. |

### Retry cycle (per row)

```
BEGIN
  SELECT * FROM payment_dead_letters
   WHERE "Status" = 'PendingRetry' AND "NextRetryAt" <= @now
   ORDER BY "NextRetryAt" LIMIT 1
   FOR UPDATE SKIP LOCKED                 -- none → end cycle
  PublishRawAsync(row.Topic, row.MessageKey, row.Payload)
    ok   → Resolved, ResolvedAt = now
    fail → RetryCount+1, LastError, LastAttemptAt = now,
           Exhausted if RetryCount >= MaxRetries (log Error)
           else NextRetryAt = now + min(Base·2^(RetryCount−1), Max)
  SaveChanges
COMMIT
```

Cancellation during a publish aborts the transaction (rollback) — the row stays `PendingRetry`
with unchanged bookkeeping and is retried later.

## Project Structure

### Documentation (this feature)

```text
specs/003-dead-letter-retry/
├── plan.md              # This file
├── research.md          # Phase 0
├── data-model.md        # Phase 1
├── quickstart.md        # Phase 1
├── contracts/
│   └── dead-letter-retry.md
├── checklists/
│   └── requirements.md
└── tasks.md             # Phase 2 (/speckit-tasks — not created here)
```

### Source Code (repository root)

```text
src/
├── Ecommerce.Payments.Consumer/
│   ├── Program.cs                                   # register worker, options, TimeProvider
│   ├── appsettings.json                             # DeadLetterRetry section
│   └── README.md                                    # hosted services
└── Ecommerce.Payments.Infrastructure/
    ├── Ecommerce.Payments.Infrastructure.csproj     # + Microsoft.Extensions.Hosting.Abstractions
    ├── DeadLetters/
    │   ├── DeadLetterRetryOptions.cs
    │   ├── DeadLetterRetryPolicy.cs
    │   ├── DeadLetterRetryProcessor.cs
    │   └── DeadLetterRetryWorker.cs
    ├── Messaging/
    │   ├── IRawEventPublisher.cs
    │   └── PaymentEventPublisher.cs                 # raw publish + repository-based dead-letter
    ├── Persistence/
    │   ├── Entities/DeadLetterStatus.cs
    │   ├── Entities/PaymentDeadLetterEntity.cs      # new columns
    │   ├── IPaymentDeadLetterRepository.cs
    │   ├── PaymentDeadLetterRepository.cs
    │   ├── PaymentsDbContext.cs
    │   └── Migrations/<ts>_AddDeadLetterRetryBookkeeping.cs
    └── README.md

tests/
├── Ecommerce.Payments.Infrastructure.Tests/         # NEW unit-test project (added to .slnx)
│   ├── DeadLetters/DeadLetterRetryPolicyTests.cs
│   └── DeadLetters/DeadLetterRetryProcessorTests.cs
├── Ecommerce.Payments.Integration.Tests/
│   └── DeadLetterClaimTests.cs                      # incl. two concurrent claimers, backfill
└── Ecommerce.Payments.Component.Tests/
    ├── DeadLetterRetryFlowTests.cs                  # first publish fails, later succeeds
    └── PublishFailureDeadLetterTests.cs             # DI updated (TimeProvider, repository); asserts new columns

docs/
└── dead-letter-retry.md                             # states, settings, example SQL INSERT, ADR 0003 link
README.md, CLAUDE.md                                 # background services, commands/notes
```

**Structure Decision**: existing four `src` projects are kept. Worker, options, policy and
processor live in `Ecommerce.Payments.Infrastructure/DeadLetters` as requested; persistence
extends `Persistence/`, publishing extends `Messaging/`. One new unit-test project is added
because existing unit-test projects do not reference Infrastructure (research R10).

## Risks

- **Existing component/integration test DI**: `PaymentEventPublisher` gains `TimeProvider` and
  the repository; tests that register it manually must add `TimeProvider.System` and
  `IPaymentDeadLetterRepository`. Planned as part of the publisher task.
- **Long cycles during an outage**: each failed publish waits up to 10 s, so a full batch can
  take ~200 s; `PeriodicTimer` simply skips missed ticks. Acceptable for an outage path.
- **Migration on populated tables**: backfill is a single `UPDATE`; dead-letter volume is tiny.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Retry worker uses Repository + Publisher directly, without Consumer/Service/Domain layers | It consumes nothing from Kafka and applies no business rule: it re-sends an already-persisted, already-serialized event. Placement in Infrastructure was requested. | Routing through a Service handler and Domain would invent a fake "dead-letter aggregate" with no business invariants and would require Service to depend on Infrastructure types (topic, key, raw payload). |
| Fifth test project (`Ecommerce.Payments.Infrastructure.Tests`) | Pure unit tests for retry policy/processor that live in Infrastructure. | Putting them in Integration.Tests mixes container-free unit tests with container tests (Principle V layers). |
