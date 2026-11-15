# Implementation Plan: Transactional Outbox for Payment Events

**Branch**: `feature/US-07-transactional-outbox` | **Date**: 2026-11-15 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/007-transactional-outbox/spec.md`

## Summary

Each payment event is saved in the same transaction as the payment change that produced it, so
it can no longer be lost. A background relay publishes it afterwards.

- **Write path**: both handlers already run their read-decide-write step in the per-order
  transaction (US-05). They now add the integration event to `payment_outbox` through a new
  Service port, `IPaymentOutbox`, before committing. They never publish, and
  `IPaymentEventPublisher` is deleted.
- **Store and relay (Q1 = A)**: the US-03 table and worker become the outbox and its relay.
  - One migration renames `payment_dead_letters` to `payment_outbox` and maps the statuses
    (`PendingRetry`→`Pending`, `Resolved`→`Published`).
  - It adds a unique `EventId` and a `Sequence` for per-payment ordering.
  - Entries that are in flight carry over unchanged.
- **Relay behaviour**:
  - It polls every 250 ms.
  - It claims one entry per transaction with `FOR UPDATE SKIP LOCKED`. An entry is eligible only
    when every earlier entry for the same payment is published.
  - It publishes the stored payload unchanged and marks the entry `Published`, or backs off
    until `Exhausted`.
- **Unchanged**: event contracts and at-least-once delivery.
- **Documentation**: ADR 0006 supersedes ADR 0003.

## Technical Context

**Language/Version**: C# / .NET 10

**Primary Dependencies**: Confluent.Kafka 2.15.0, EF Core 10.0.4 + Npgsql 10.0.2, Polly 8.7.0.
No new packages.

**Storage**: PostgreSQL. `payment_dead_letters` becomes `payment_outbox` (rename, status
mapping, `EventId`, `Sequence`, one dropped column). Other tables are unchanged.

**Testing**: xUnit, Moq, `FakeTimeProvider`, Testcontainers (PostgreSQL, Kafka)

**Target Platform**: .NET Worker Service

**Project Type**: Event-driven background worker, no HTTP surface

**Performance Goals**:
- Event visible under 1 s at p95, at most 2 s, when healthy.
- When idle, four indexed polls per second per instance.

**Constraints**:
- An outcome is never saved without its event.
- Order is kept per payment.
- At most one instance publishes a given entry at a time.
- Startup does not depend on Kafka.
- Contracts are unchanged.

**Scale/Scope**:
- Write path in two handlers.
- US-03 components renamed and extended.
- One migration.
- About 38 files touched, most of them tests.

No open NEEDS CLARIFICATION. See [research.md](research.md).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Checked against v1.3.0.

| Principle | Check | Status |
|---|---|---|
| I. Service | Calls Domain, then saves the payment and `IPaymentOutbox` entries in one transaction. No Publisher or Kafka reference remains; `IPaymentEventPublisher` is deleted. | ✅ |
| I. Repository | The payment change and the outbox entry are written in the same `IOrderUnitOfWork` transaction. Neither is saved without the other. | ✅ |
| I. Publisher | Serialisation happens when the entry is recorded (`PaymentOutbox`). `PaymentEventPublisher` is only `IRawEventPublisher`, and only the relay calls it. | ✅ |
| I. Consumer | Commits after the handler returns, which is after the transaction commits. It no longer waits for a publish. | ✅ |
| II. Write flow | Steps 4 to 6 follow v1.3.0. A failed transaction saves and publishes nothing. A failed publish is a relay retry. A crash at any point loses nothing (`OutboxCrashRecoveryTests`). | ✅ |
| II. Idempotency | Unchanged (US-01, US-04, US-05). A relay re-publish keeps the same `eventId`, which is unique in the outbox. | ✅ |
| III. Type safety | Typed entity and `OutboxStatus` enum. Integration events stay typed until serialised. | ✅ |
| IV/V. Tests | Unit tests for the handlers and the relay components. Integration tests for atomicity, claim and ordering, and the migration. Component tests for crash recovery, latency, ordering and the existing flows. | ✅ |
| VI. Publishing rules | Only from the outbox. Order per payment (R4 query). 2 s healthy bound (250 ms poll). One failing payment does not block others. | ✅ |
| VI. Failure handling | A single outbound store (`payment_outbox`). No separate dead-letter path remains. | ✅ |
| VI. Outbox relay | `Pending` / `Published` / `Exhausted`, retry count, next attempt, last error, recorded and published times, `SKIP LOCKED`, payload and `eventId` unchanged, published entries kept. | ✅ |
| VI. Startup | The relay yields before any I/O, as today. | ✅ |
| VII. Branching | Branch `feature/US-07-transactional-outbox`, taken from an up-to-date `main`. Commits use `feat(US-07)`, `refactor(US-07)`, `test(US-07)` and `docs(US-07)`. | ✅ |
| VIII. Quality gate | Build, format and test after every task. | ✅ |
| Docs | ADR 0006 (supersedes 0003). `docs/outbox.md` replaces `docs/dead-letter-retry.md`. README background-services table, configuration and write flow. Project READMEs and `CLAUDE.md`. | ✅ |

**Post-design re-check**: unchanged, all ✅.

## Design

### Handler (both handlers, same shape)

```
retry(async () => {
  await using tx = unitOfWork.BeginAsync(orderId)        // US-05 lock
  … existing reads / Domain decision …
  repository.Save…(payment)
  outbox.Add…Async(domainEvent)                           // same transaction (FR-001)
  tx.CommitAsync()
})
// nothing published here (FR-002)
```

### Relay cycle (`OutboxRelayProcessor.RunCycleAsync`)

```
repeat up to BatchSize:
  tx = repository.BeginTransactionAsync()
  entry = repository.ClaimNextDueAsync(now)               // R4: due, oldest Sequence, no unpublished predecessor, SKIP LOCKED
  none → commit; stop
  try   rawPublisher.PublishRawAsync(entry.Topic, entry.MessageKey, entry.Payload)
        policy.ApplySuccess(entry, now)                   // Published, PublishedAt
  catch policy.ApplyFailure(entry, error, now, options)   // backoff or Exhausted (+ log blocked count)
  save; commit
```

`OutboxRelayWorker` runs a cycle every `PollIntervalMs` (250). The first cycle runs immediately
after `Task.Yield`.

### Components

| Component | Project / path | Kind |
|---|---|---|
| `IPaymentOutbox` | Service `Payments/` | new |
| `IPaymentEventPublisher` | Service `Payments/` | delete |
| `ProcessPaymentHandler`, `RefundPaymentHandler` | Service `Payments/` | change |
| `PaymentOutbox` | Infrastructure `Outbox/` | new (serialisation moved from the publisher) |
| `OutboxRelayWorker`, `OutboxRelayProcessor`, `IOutboxRelayProcessor`, `OutboxRetryPolicy`, `OutboxRelayOptions` | Infrastructure `Outbox/` | renamed from `DeadLetters/` and extended |
| `OutboxEntryEntity`, `OutboxStatus`, `IPaymentOutboxRepository`, `PaymentOutboxRepository` | Infrastructure `Persistence/` | renamed from the dead-letter types and extended |
| `PaymentEventPublisher` | Infrastructure `Messaging/` | reduced to `IRawEventPublisher` |
| `PaymentsDbContext` | Infrastructure `Persistence/` | mapping |
| Migration `TurnDeadLettersIntoOutbox` | Infrastructure `Persistence/Migrations/` | new, with hand-written SQL steps |
| DI and settings | Consumer `Program.cs`, `appsettings.json` | `OutboxRelay` section, `IPaymentOutbox`, relay hosted service |
| ADR 0006, `docs/outbox.md`, README, project READMEs, `CLAUDE.md` | docs | new / change |

## Project Structure

### Documentation (this feature)

```text
specs/007-transactional-outbox/
├── spec.md  plan.md  research.md  data-model.md  quickstart.md
├── contracts/outbound-events.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
src/
├── Ecommerce.Payments.Service/Payments/IPaymentOutbox.cs                               (new)
│   IPaymentEventPublisher.cs                                                           (delete)
│   ProcessPaymentHandler.cs  RefundPaymentHandler.cs                                   (change)
├── Ecommerce.Payments.Infrastructure/
│   ├── Outbox/  PaymentOutbox.cs (new)  OutboxRelayWorker.cs  OutboxRelayProcessor.cs
│   │            IOutboxRelayProcessor.cs  OutboxRetryPolicy.cs  OutboxRelayOptions.cs  (renamed from DeadLetters/)
│   ├── Persistence/Entities/OutboxEntryEntity.cs  OutboxStatus.cs                      (renamed)
│   ├── Persistence/IPaymentOutboxRepository.cs  PaymentOutboxRepository.cs            (renamed)
│   ├── Persistence/PaymentsDbContext.cs                                                (change)
│   ├── Persistence/Migrations/<ts>_TurnDeadLettersIntoOutbox.cs                        (new)
│   └── Messaging/PaymentEventPublisher.cs                                              (reduce)
└── Ecommerce.Payments.Consumer/Program.cs  appsettings.json  README.md                 (change)

tests/
├── Ecommerce.Payments.Service.Tests/Payments/*HandlerTests.cs                          (change)
├── Ecommerce.Payments.Infrastructure.Tests/Outbox/*                                    (renamed from DeadLetters/)
├── Ecommerce.Payments.Integration.Tests/OutboxReader.cs                                (new; replaces FakePaymentEventPublisher)
│   OutboxAtomicityTests.cs  OutboxClaimTests.cs  OutboxMigrationTests.cs               (new; the last two replace DeadLetterClaim/MigrationTests)
│   existing tests                                                                      (assertions via OutboxReader)
└── Ecommerce.Payments.Component.Tests/OutboxCrashRecoveryTests.cs  OutboxLatencyTests.cs
    OutboxOrderingTests.cs                                                              (new)
    existing flow tests                                                                 (host OutboxRelayWorker)
    PublishFailureDeadLetterTests.cs  DeadLetterRetryFlowTests.cs                       (rewritten as outbox retry tests)

docs/adr/0006-transactional-outbox.md  docs/outbox.md                                   (new)
docs/dead-letter-retry.md                                                               (delete)
```

**Structure Decision**: keep the existing projects; add none.

## Risks

- **Broad rename.** About 38 files change, and most are tests.
  - *Mitigation*: use one refactor phase with the full suite green before any behaviour change.
    The behaviour assertions in tests stay the same, and only how they observe events changes.
- **Hand-written migration SQL** (backfills, identity restart, status mapping).
  - *Mitigation*: `OutboxMigrationTests` seeds rows of each status in the old shape, then
    migrates and asserts the result. `Down` is tested as well.
- **Polling load.** Four queries per second per instance, using the partial index. This is
  acceptable. A wake-up signal is a possible later optimisation.
- **Exhausted head entries block their payment's later events.** This is intended (ordering).
  Operators are told through the Error log, and the blocked count is in the message. Re-queueing
  is documented.
- **Deployment order.** Old code must not run against the migrated table. With a single
  instance, stop, migrate and start. Document this in the README upgrade note.
- **Config rename** `DeadLetterRetry` → `OutboxRelay`. There are only local deployments, and it
  is documented. The old section is ignored.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Pre-existing: `OrderPlacedConsumer` dedupes by order id, not `eventId` | Unchanged by this story; planned as US-08 | Separate follow-up |
