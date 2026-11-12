---

description: "Task list for US-03 Dead-letter retry"
---

# Tasks: Dead-Letter Retry

**Input**: Design documents from `specs/003-dead-letter-retry/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/dead-letter-retry.md,
quickstart.md (all present)

**Tests**: Included and REQUIRED — the feature request asks for unit tests (backoff and retry
decisions with a fake clock), integration tests (claiming query incl. two concurrent claimers)
and a component test (first publish fails, later succeeds); Constitution Principles IV/V also
require them.

**Organization**: Tasks are grouped by user story (spec.md) so each story is an independently
testable increment.

**Quality gate**: after **every** task run `dotnet build` → `dotnet format --verify-no-changes`
→ `dotnet test` (Constitution VIII). Docker must be running for Integration/Component tests.

**Commits**: Conventional Commits scoped `US-03`, e.g. `feat(US-03): ...`, `test(US-03): ...`,
`docs(US-03): ...`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: US1 / US2 / US3 from spec.md
- **(new)** marks files that do not exist yet; everything else extends an existing file

---

## Phase 1: Setup

**Purpose**: Package references and the new unit-test project (research R10).

- [X] T001 Add `Microsoft.Extensions.Hosting.Abstractions` 10.0.4 — aligned with the pinned Microsoft.Extensions.Options 10.0.4 to avoid an NU1605 downgrade error — (needed for `BackgroundService`) to `src/Ecommerce.Payments.Infrastructure/Ecommerce.Payments.Infrastructure.csproj`
- [X] T002 Create the unit-test project **(new)** `tests/Ecommerce.Payments.Infrastructure.Tests/Ecommerce.Payments.Infrastructure.Tests.csproj` mirroring `tests/Ecommerce.Payments.Service.Tests/Ecommerce.Payments.Service.Tests.csproj` (net10.0, xUnit 2.9.3, xunit.runner.visualstudio 3.1.4, Microsoft.NET.Test.Sdk 17.14.1, coverlet.collector 6.0.4, Moq 4.20.72, `Microsoft.Extensions.TimeProvider.Testing` 10.11.0, global `using Xunit`), referencing `src/Ecommerce.Payments.Infrastructure`, and add it to the `/tests/` folder in `Ecommerce.Payments.slnx`
- [X] T003 [P] Add `Microsoft.Extensions.TimeProvider.Testing` 10.11.0 to `tests/Ecommerce.Payments.Component.Tests/Ecommerce.Payments.Component.Tests.csproj` and `tests/Ecommerce.Payments.Integration.Tests/Ecommerce.Payments.Integration.Tests.csproj`

**Checkpoint**: solution builds, all existing tests pass.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Schema, repository extraction, raw publish method and settings shared by every
story. Normal write-flow behaviour stays identical (FR-015).

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T004 [P] Add the `DeadLetterStatus` enum **(new)** (`PendingRetry`, `Resolved`, `Exhausted`) with XML docs describing each state in `src/Ecommerce.Payments.Infrastructure/Persistence/Entities/DeadLetterStatus.cs`
- [X] T005 Extend `PaymentDeadLetterEntity` with `Topic` (string, required), `MessageKey` (string, required), `Status` (`DeadLetterStatus`), `RetryCount` (int), `NextRetryAt`, `LastAttemptAt`, `ResolvedAt` (`DateTimeOffset?`), `LastError` (string?), with XML docs on every property; replace the class summary's "replay is out of scope" sentence with a reference to US-03 and ADR 0003, in `src/Ecommerce.Payments.Infrastructure/Persistence/Entities/PaymentDeadLetterEntity.cs` (depends on T004)
- [X] T006 Map the new columns in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentsDbContext.cs`: `Status` → `HasConversion<string>().HasMaxLength(20)`, `RetryCount` default 0, and partial index `IX_payment_dead_letters_due` on `NextRetryAt` with filter `"Status" = 'PendingRetry'` (depends on T005)
- [X] T007 Generate the single migration `AddDeadLetterRetryBookkeeping` with `dotnet ef migrations add AddDeadLetterRetryBookkeeping --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`, then edit its `Up` to add a backfill `migrationBuilder.Sql(...)` after the column adds: `Status = 'PendingRetry'`, `RetryCount = 0`, `NextRetryAt = now()`, `MessageKey = "PaymentId"::text`, `Topic = CASE "EventType" WHEN 'PaymentProcessedEvent' THEN 'payments.payment-processed' WHEN 'PaymentFailedEvent' THEN 'payments.payment-failed' ELSE '' END` (data-model.md "Migration backfill"); `Down` drops the index and columns — in `src/Ecommerce.Payments.Infrastructure/Persistence/Migrations/` (depends on T006)
- [X] T008 Create `IPaymentDeadLetterRepository` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/IPaymentDeadLetterRepository.cs` with XML docs: `AddAsync(PaymentDeadLetterEntity, CancellationToken)`, `BeginTransactionAsync(CancellationToken)` (returns `IDbContextTransaction`), `ClaimDueAsync(DateTimeOffset now, int limit, CancellationToken)` (returns tracked rows; MUST be called inside a transaction), `SaveChangesAsync(CancellationToken)`
- [X] T009 Implement `PaymentDeadLetterRepository` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentDeadLetterRepository.cs` over `PaymentsDbContext`: `AddAsync` adds + saves; `ClaimDueAsync` uses `PaymentDeadLetters.FromSql($"SELECT * FROM payment_dead_letters WHERE \"Status\" = 'PendingRetry' AND \"NextRetryAt\" <= {now} ORDER BY \"NextRetryAt\" LIMIT {limit} FOR UPDATE SKIP LOCKED").ToListAsync(ct)` and throws `InvalidOperationException` if `Database.CurrentTransaction` is null (depends on T005, T008)
- [X] T010 [P] Create `IRawEventPublisher` **(new)** with `PublishRawAsync(string topic, string key, string payload, CancellationToken)` — XML docs state it throws on failure and never writes a dead letter — in `src/Ecommerce.Payments.Infrastructure/Messaging/IRawEventPublisher.cs`
- [X] T011 Refactor `PaymentEventPublisher` in `src/Ecommerce.Payments.Infrastructure/Messaging/PaymentEventPublisher.cs`: implement `IRawEventPublisher` (`ProduceAsync` with the given topic/key/value, exceptions propagate); `PublishAsync`/`PublishFailedAsync` serialize exactly as today and call `PublishRawAsync` inside the existing try/catch; on failure write the dead letter through `IPaymentDeadLetterRepository` (resolved from a new scope) with `Topic`, `MessageKey` = payment id, `Status = PendingRetry`, `RetryCount = 0`, `CreatedAt = NextRetryAt = TimeProvider.GetUtcNow()`; inject `TimeProvider`; delete the inline `WriteDeadLetterAsync` DbContext code; update class XML docs (depends on T008, T009, T010)
- [X] T012 [P] Create `DeadLetterRetryOptions` **(new)** (`SectionName = "DeadLetterRetry"`, `IntervalMs` 30000, `BatchSize` 20, `BaseDelayMs` 5000, `MaxDelayMs` 600000, `MaxRetries` 8, XML docs per property) in `src/Ecommerce.Payments.Infrastructure/DeadLetters/DeadLetterRetryOptions.cs`
- [X] T013 Update DI in `src/Ecommerce.Payments.Consumer/Program.cs`: `AddSingleton(TimeProvider.System)`; `AddScoped<IPaymentDeadLetterRepository, PaymentDeadLetterRepository>()`; register `PaymentEventPublisher` once as singleton and forward `IPaymentEventPublisher` and `IRawEventPublisher` to it; bind `DeadLetterRetryOptions` with `.Validate(o => o.IntervalMs > 0 && o.BatchSize > 0 && o.BaseDelayMs > 0 && o.MaxRetries > 0 && o.MaxDelayMs >= o.BaseDelayMs).ValidateOnStart()` (depends on T011, T012)
- [X] T014 [P] Add the `DeadLetterRetry` section with the defaults from contracts/dead-letter-retry.md §2 to `src/Ecommerce.Payments.Consumer/appsettings.json`
- [X] T015 Update every test that constructs or registers `PaymentEventPublisher` to also register `TimeProvider.System` and `IPaymentDeadLetterRepository` (start with `tests/Ecommerce.Payments.Component.Tests/PublishFailureDeadLetterTests.cs`; grep `PaymentEventPublisher` under `tests/` for the rest); in `PublishFailureDeadLetterTests` additionally assert `Status == PendingRetry`, `RetryCount == 0`, `Topic == "payments.payment-processed"`, `MessageKey == payment.Id.ToString()`, `NextRetryAt` not null (depends on T011)
- [X] T016 [P] Add integration test **(new)** `MigrationBackfill_ExistingRow_BecomesPendingRetryWithTopicAndKey` in `tests/Ecommerce.Payments.Integration.Tests/DeadLetterMigrationTests.cs`: migrate a PostgreSQL container to `AddPaymentFailureFields` (via `IMigrator.MigrateAsync(targetMigration)`), insert a legacy row with raw SQL for each of `PaymentProcessedEvent`, `PaymentFailedEvent` and an unknown type, migrate to latest, assert status/retry count/`NextRetryAt`/topic/key per data-model.md (depends on T007)

**Checkpoint**: schema and plumbing in place; the normal flow behaves exactly as before and
new dead letters carry bookkeeping. Quality gate green.

---

## Phase 3: User Story 1 — Deliver a parked payment outcome once the broker is back (Priority: P1) 🎯 MVP

**Goal**: a due `PendingRetry` row is republished unchanged (topic, key, `eventId`) and marked
`Resolved`; non-due and terminal rows are never touched.

**Independent Test**: insert a due row with Kafka available → within one interval the message
is on its topic with the same `eventId`/key and the row is `Resolved` with `ResolvedAt`.

### Tests for User Story 1

- [X] T017 [P] [US1] Unit tests **(new)** in `tests/Ecommerce.Payments.Infrastructure.Tests/DeadLetters/DeadLetterRetryPolicyTests.cs`: `Should_Mark_Resolved_With_ResolvedAt_And_LastAttemptAt_On_Success` (fake clock), `Should_Not_Change_Payload_Topic_Key_Or_FailureReason_On_Success`
- [X] T018 [P] [US1] Unit tests **(new)** in `tests/Ecommerce.Payments.Infrastructure.Tests/DeadLetters/DeadLetterRetryProcessorTests.cs` (Moq `IPaymentDeadLetterRepository` + `IRawEventPublisher`, `FakeTimeProvider`): `Should_Publish_Stored_Topic_Key_And_Payload_Unchanged`, `Should_Resolve_And_Commit_After_Successful_Publish`, `Should_Stop_Cycle_When_No_Due_Row_Is_Claimed`, `Should_Process_At_Most_BatchSize_Rows_Per_Cycle`, `Should_Pass_Fake_Clock_Now_To_Claim`
- [X] T019 [P] [US1] Integration tests **(new)** in `tests/Ecommerce.Payments.Integration.Tests/DeadLetterClaimTests.cs` (PostgreSQL container, migrated): `Should_Claim_Due_PendingRetry_Row`, `Should_Not_Claim_Row_With_Future_NextRetryAt`, `Should_Not_Claim_Resolved_Or_Exhausted_Rows`, `Should_Claim_Oldest_NextRetryAt_First_Up_To_Limit`, `Should_Throw_When_Claiming_Outside_A_Transaction`
- [X] T020 [P] [US1] Component test **(new)** `Should_Republish_DeadLettered_Event_With_Same_EventId_And_Key_And_Resolve_Row` in `tests/Ecommerce.Payments.Component.Tests/DeadLetterRetryFlowTests.cs` (Kafka + PostgreSQL containers): run `OrderPlacedConsumer.HandleMessageAsync` with a publisher pointed at an unreachable broker (`127.0.0.1:1`) so the first publish genuinely fails and creates the row; then run `DeadLetterRetryProcessor.RunCycleAsync` with a publisher pointed at the Kafka container; consume `payments.payment-processed` (reuse `KafkaTestConsumer`) and assert the message value equals the stored payload, key = payment id, `eventId` unchanged, row `Resolved` with `ResolvedAt`

### Implementation for User Story 1

- [X] T021 [US1] Create `DeadLetterRetryPolicy` **(new)** static class with `ApplySuccess(PaymentDeadLetterEntity row, DateTimeOffset now)` (Status = Resolved, ResolvedAt = LastAttemptAt = now) and XML docs, in `src/Ecommerce.Payments.Infrastructure/DeadLetters/DeadLetterRetryPolicy.cs`
- [X] T022 [US1] Create `DeadLetterRetryProcessor` **(new)** in `src/Ecommerce.Payments.Infrastructure/DeadLetters/DeadLetterRetryProcessor.cs` (ctor: `IPaymentDeadLetterRepository`, `IRawEventPublisher`, `IOptions<DeadLetterRetryOptions>`, `TimeProvider`, `ILogger`): `RunCycleAsync(CancellationToken)` loops up to `BatchSize` times — begin transaction → `ClaimDueAsync(now, 1)` → none: commit and stop → `PublishRawAsync(row.Topic, row.MessageKey, row.Payload)` → `ApplySuccess` → save → commit, log Information (contract §4); returns rows processed. For now a publish failure logs a Warning and rolls back the transaction (failure bookkeeping arrives in US2, T027) (depends on T009, T011, T021)
- [X] T023 [US1] Create `DeadLetterRetryWorker : BackgroundService` **(new)** in `src/Ecommerce.Payments.Infrastructure/DeadLetters/DeadLetterRetryWorker.cs` (ctor: `IServiceScopeFactory`, `IOptions<DeadLetterRetryOptions>`, `TimeProvider`, `ILogger`): `ExecuteAsync` does `await Task.Yield()`, then loops — new scope → resolve processor → `RunCycleAsync(stoppingToken)` inside try/catch (`OperationCanceledException` when stopping → exit; any other exception → log Error, continue) → `await timer.WaitForNextTickAsync(stoppingToken)` with `new PeriodicTimer(TimeSpan.FromMilliseconds(IntervalMs), timeProvider)`; dispose the timer; XML docs cite FR-012 and ADR 0003 (depends on T022)
- [X] T024 [US1] Register `AddScoped<DeadLetterRetryProcessor>()` and `AddHostedService<DeadLetterRetryWorker>()` in `src/Ecommerce.Payments.Consumer/Program.cs` (depends on T013, T023)
- [X] T025 [P] [US1] Unit tests **(new)** in `tests/Ecommerce.Payments.Infrastructure.Tests/DeadLetters/DeadLetterRetryWorkerTests.cs` (`FakeTimeProvider`, mocked processor via a scope factory): `Should_Return_From_StartAsync_Without_Running_A_Cycle_Synchronously`, `Should_Run_Next_Cycle_After_Interval_Elapses`, `Should_Keep_Running_After_A_Cycle_Throws`, `Should_Stop_When_Cancellation_Is_Requested` — make `RunCycleAsync` `virtual` or extract `IDeadLetterRetryProcessor` (with XML docs) if needed for mocking (depends on T023)

**Checkpoint**: MVP — dead letters are delivered after an outage. Quality gate green.

---

## Phase 4: User Story 2 — Back off and give up cleanly (Priority: P2)

**Goal**: failed retries are recorded on the existing row with capped exponential backoff and
end `Exhausted` after `MaxRetries` with an error log.

**Independent Test**: with a failing publisher and a fake clock, a row's `NextRetryAt` moves out
5 s, 10 s, 20 s… and it is `Exhausted` with `RetryCount == MaxRetries`; no new row appears.

### Tests for User Story 2

- [X] T026 [P] [US2] Extend `tests/Ecommerce.Payments.Infrastructure.Tests/DeadLetters/DeadLetterRetryPolicyTests.cs`: `[Theory]` `Should_Double_Delay_From_Base_Per_Retry` (retry 1→5 s, 2→10 s, 3→20 s, 7→320 s with defaults), `Should_Cap_Delay_At_MaxDelay` (e.g. retry 20 → 600 s; retry 1000 does not overflow), `Should_Increment_RetryCount_And_Record_LastError_And_LastAttemptAt_On_Failure`, `Should_Set_NextRetryAt_To_Now_Plus_Backoff_On_Failure`, `Should_Mark_Exhausted_When_RetryCount_Reaches_MaxRetries`, `Should_Remain_PendingRetry_Below_MaxRetries`
- [X] T027 [P] [US2] Extend `tests/Ecommerce.Payments.Infrastructure.Tests/DeadLetters/DeadLetterRetryProcessorTests.cs`: `Should_Record_Failure_On_Existing_Row_And_Commit`, `Should_Never_Add_A_New_DeadLetter_On_Retry_Failure` (verify `AddAsync` never called), `Should_Log_Error_With_Full_Context_When_Exhausted`, `Should_Continue_With_Next_Row_After_A_Failed_Publish`, `Should_Roll_Back_Without_Bookkeeping_When_Cancelled_During_Publish`
- [X] T028 [P] [US2] Component test **(new case)** `Should_Back_Off_After_Failed_Retry_Then_Resolve_When_Publish_Succeeds` in `tests/Ecommerce.Payments.Component.Tests/DeadLetterRetryFlowTests.cs`: a test `IRawEventPublisher` decorator that throws on its first call then delegates to a real `PaymentEventPublisher` on the Kafka container; `FakeTimeProvider`; cycle 1 → `RetryCount 1`, `NextRetryAt = now + 5 s`; cycle at now + 4 s → row not claimed; advance to + 5 s → published, `Resolved`, same `eventId`

### Implementation for User Story 2

- [X] T029 [US2] Add `ComputeDelay(int retryCount, DeadLetterRetryOptions options)` (`min(Base × 2^(retryCount−1), Max)`, cap before multiplying to avoid overflow) and `ApplyFailure(row, string error, DateTimeOffset now, options)` returning `bool exhausted` (RetryCount+1, LastError, LastAttemptAt; Exhausted when RetryCount ≥ MaxRetries, else NextRetryAt = now + delay) to `src/Ecommerce.Payments.Infrastructure/DeadLetters/DeadLetterRetryPolicy.cs`
- [X] T030 [US2] Replace the US1 rollback-on-failure in `src/Ecommerce.Payments.Infrastructure/DeadLetters/DeadLetterRetryProcessor.cs`: on publish exception (not cancellation) call `ApplyFailure`, save, commit; log Warning with retry count, next retry time and error, or Error with dead-letter id, payment id, event type, topic, retry count and last error when exhausted; on `OperationCanceledException` roll back and rethrow (depends on T029)

**Checkpoint**: persistent failures back off and terminate. Quality gate green.

---

## Phase 5: User Story 3 — Run safely on several instances (Priority: P3)

**Goal**: concurrent workers never claim the same row and never wait on each other's locks.

**Independent Test**: two concurrent claim transactions over the same due rows return disjoint
sets, and the second returns immediately while the first still holds its locks.

### Tests for User Story 3

- [X] T031 [P] [US3] Extend `tests/Ecommerce.Payments.Integration.Tests/DeadLetterClaimTests.cs`: `Two_Concurrent_Claimers_Get_Disjoint_Rows` (two `PaymentsDbContext`s, each with an open transaction, claim limit 3 over 6 due rows → no shared ids, union = 6), `Second_Claimer_Skips_Locked_Row_Without_Waiting` (first holds 1 row; second with `SET LOCAL lock_timeout = '1s'` returns 0 rows in < 1 s, no exception), `Row_Is_Claimable_Again_After_First_Transaction_Rolls_Back`
- [X] T032 [P] [US3] Integration test **(new case)** `Two_Processors_In_Parallel_Publish_Each_Row_Exactly_Once` in `tests/Ecommerce.Payments.Integration.Tests/DeadLetterClaimTests.cs`: two `DeadLetterRetryProcessor` instances (separate scopes/DbContexts) with a recording fake `IRawEventPublisher` that delays each publish ~100 ms, run `RunCycleAsync` concurrently over 10 due rows → every row published exactly once and all `Resolved`

### Implementation for User Story 3

- [X] T033 [US3] Fix anything T031/T032 reveal in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentDeadLetterRepository.cs` / `DeadLetterRetryProcessor.cs` (expected: none — SKIP LOCKED and per-row transactions come from T009/T022); document the multi-instance guarantee in both classes' XML docs

**Checkpoint**: safe to scale out. Quality gate green.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [X] T034 [P] Write `docs/dead-letter-retry.md` **(new)**: purpose, link to `docs/adr/0003-dead-letter-retry.md`, state diagram and meaning of `PendingRetry`/`Resolved`/`Exhausted`, the settings table with defaults and env-var overrides, backoff schedule with defaults, multi-instance claiming, at-least-once/`eventId` note, out-of-scope items, useful queries (list exhausted rows), and an example SQL `INSERT INTO payment_dead_letters (...)` creating a due `PendingRetry` `PaymentProcessedEvent` row with a valid JSON payload
- [X] T035 [P] Update `src/Ecommerce.Payments.Infrastructure/README.md` (DeadLetters folder, `PaymentDeadLetterRepository`, `IRawEventPublisher`) and `src/Ecommerce.Payments.Consumer/README.md` (hosted services incl. `DeadLetterRetryWorker`, `DeadLetterRetry` settings)
- [X] T036 [P] Update root `README.md`: Background services table → dead-letter retry worker "Implemented (US-03)", configuration reference rows for `DeadLetterRetry:*`, link to `docs/dead-letter-retry.md`; add `tests/Ecommerce.Payments.Infrastructure.Tests/` to the project layout
- [X] T037 [P] Update `CLAUDE.md`: dead-letter retry worker and settings in the write-flow section (link `docs/dead-letter-retry.md`, ADR 0003), the new test project, and the rule that retry paths use `IRawEventPublisher` (never the dead-lettering methods)
- [X] T038 Verify XML doc comments exist on every new/changed public type and member in `src/Ecommerce.Payments.Infrastructure/DeadLetters/`, `src/Ecommerce.Payments.Infrastructure/Persistence/` and `src/Ecommerce.Payments.Infrastructure/Messaging/`
- [ ] T039 **(partially done — see note)** Run the manual scenario in `specs/003-dead-letter-retry/quickstart.md` §2 against ecommerce-infra (insert via the docs SQL, confirm `Resolved` and the message on the topic; stop Kafka and observe backoff; start the service with Kafka down)
- [X] T040 Final quality gate: `dotnet build`, `dotnet format --verify-no-changes`, `dotnet test` all green; `docker build -t ecommerce-payments .` still succeeds

---

## Dependencies & Execution Order

### Phase dependencies

- **Setup (T001–T003)** → **Foundational (T004–T016)** → **US1 (T017–T025)** → **US2 (T026–T030)** → **US3 (T031–T033)** → **Polish (T034–T040)**
- US2 extends the processor and policy from US1 (T029/T030 depend on T021/T022).
- US3 adds verification over US1's claim/processor; it can start right after US1, in parallel
  with US2 (T032 only needs `RunCycleAsync`).

### Within Foundational

T004 → T005 → T006 → T007 → T016; T005 + T008 → T009; T009 + T010 → T011 → T013 / T015;
T012 → T013. T014 is independent.

### Within each story

Write the tests first (they fail), then implement until green: US1 T017–T020 → T021 → T022 →
T023 → T024 → T025; US2 T026–T028 → T029 → T030; US3 T031–T032 → T033.

## Parallel Examples

```text
# Foundational, after T005:
T008 IPaymentDeadLetterRepository   |  T010 IRawEventPublisher  |  T012 DeadLetterRetryOptions  |  T014 appsettings

# US1 tests (different files):
T017 policy tests  |  T018 processor tests  |  T019 claim integration tests  |  T020 component test

# US2 tests:
T026 policy tests  |  T027 processor tests  |  T028 component test

# Polish docs:
T034 docs/dead-letter-retry.md  |  T035 project READMEs  |  T036 root README  |  T037 CLAUDE.md
```

## Implementation Strategy

1. **MVP**: Setup + Foundational + US1 → dead letters are delivered after an outage (rows that
   still fail are simply retried next interval). Demo with quickstart §2 step 1–4.
2. **US2**: adds backoff and termination — required before production so a dead broker or bad
   row is not retried every 30 s forever.
3. **US3**: proves multi-instance safety; mostly tests, since the design already uses SKIP
   LOCKED.
4. **Polish**: docs, README/CLAUDE.md, manual validation, final gate.

Commit after each task or logical group, with the quality gate green every time.

## Implementation notes

- **T039 (manual scenario) — partially done.** The example SQL from `docs/dead-letter-retry.md`
  was run against a throwaway PostgreSQL container migrated to the latest schema (INSERT and
  both queries succeed; the row is a due `PendingRetry`). The end-to-end scenarios (republish to
  real Kafka, backoff then success, startup with an unreachable broker) are covered by the
  Component tests. The steps against the shared `ecommerce-infra` stack (applying the migration
  to the shared database, stopping the shared Kafka broker) were **not** run, because a local
  `Ecommerce.Payments.Consumer` instance was already running against that stack and stopping
  Kafka would affect the other services. Run quickstart.md §2 manually to close this task.
- **Quality gate configuration**: the running local Consumer process locked
  `src/Ecommerce.Payments.Consumer/bin/Debug`, so the gate was run with `-c Release`
  (`dotnet build -c Release`, `dotnet format --verify-no-changes`, `dotnet test -c Release`).
- **Bug found and fixed (T020)**: the single `PaymentEventPublisher` instance is registered under
  three service types, and the DI container disposed it once per registration; the second
  `Dispose` threw from `Producer.Flush`. `Dispose` is now idempotent.
- **T001**: `Microsoft.Extensions.Hosting.Abstractions` pinned to 10.0.4 instead of 10.0.9 to
  match the existing `Microsoft.Extensions.Options` 10.0.4 (10.0.9 caused an NU1605 downgrade
  error treated as an error).
- **T025**: `IDeadLetterRetryProcessor` was extracted so the worker can be tested with a mock.
