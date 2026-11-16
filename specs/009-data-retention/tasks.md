---

description: "Task list for US-09 Data Retention"
---

# Tasks: Data Retention for Outbox, Dead Letters and Cancellation Markers

**Input**: Design documents from `specs/009-data-retention/`.

**Prerequisites**: plan.md, spec.md, research.md, data-model.md and quickstart.md, all present.
There is no `contracts/` folder because nothing external changes.

**Tests**: Included and required (Constitution IV/V). quickstart.md maps each scenario to a test.

**Organization**: tasks are grouped by the user stories in spec.md:
- **US1**: purge old published outbox entries (P1).
- **US2**: purge old dead letters and cancellation markers (P2).
- **US3**: purge safely and continuously on any number of instances (P2).

**No Setup phase**: there are no new projects or packages.

**Quality gate**: after **every** task, run in order:
1. `dotnet build`
2. `dotnet format --verify-no-changes`
3. `dotnet test` (needs Docker)

**Commits**: Conventional Commits scoped `US-09`.

## Format: `[ID] [P?] [Story] Description`

---

## Phase 1: Foundational (Blocking Prerequisites)

**Purpose**: the options, the result type, the purger skeleton, the indexes and the DI wiring.
Nothing is purged yet.

- [ ] T001 [P] Create `RetentionOptions` **(new)** in `src/Ecommerce.Payments.Infrastructure/Retention/RetentionOptions.cs`.
  - `SectionName = "Retention"`.
  - Properties with defaults: `IntervalMs` = 3_600_000, `BatchSize` = 1_000, `OutboxPublishedDays` = 30, `ConsumedDeadLettersDays` = 90, `CancellationMarkersDays` = 180.
  - XML docs on each property. They state that 0 means keep forever, and warn that marker retention must exceed the longest expected `OrderPlaced` delay.
  - `bool IsValid()`: `IntervalMs > 0 && BatchSize > 0` and all three day values `>= 0`.

  Also write `tests/Ecommerce.Payments.Infrastructure.Tests/Retention/RetentionOptionsTests.cs` **(new)**: the defaults are valid; zero days are valid; zero or negative interval or batch is invalid; negative days are invalid.
- [ ] T002 [P] Create `PurgeResult` **(new)** in `src/Ecommerce.Payments.Infrastructure/Retention/PurgeResult.cs`: `public sealed record PurgeResult(int OutboxEntries, int DeadLetters, int Markers)`, with a `Total` property. Create `IRetentionPurger` **(new)** in `src/Ecommerce.Payments.Infrastructure/Retention/IRetentionPurger.cs`: `Task<PurgeResult> PurgeAsync(CancellationToken)`. Give both XML docs.
- [ ] T003 Add the age indexes.
  - In `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentsDbContext.cs`, add:
    - `IX_payment_outbox_published_at` on `PublishedAt`, filtered on `"Status" = 'Published'`;
    - `IX_consumed_dead_letters_failed_at` on `FailedAt`;
    - `IX_order_cancellation_markers_recorded_at` on `RecordedAt`.
  - Generate the migration with `dotnet ef migrations add AddRetentionIndexes --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure --output-dir Persistence/Migrations`, then run `dotnet format`.
  - Check that the migration only creates and drops those three indexes.
- [ ] T004 Create `RetentionPurger` **(new)** in `src/Ecommerce.Payments.Infrastructure/Retention/RetentionPurger.cs` as `public sealed class RetentionPurger : IRetentionPurger`. Its constructor takes `PaymentsDbContext`, `IOptions<RetentionOptions>` and `TimeProvider`.
  - `PurgeAsync`:
    1. Compute `now` once.
    2. For each store, if its days value is greater than 0, compute `cutoff = now.AddDays(-days)` and call a private `PurgeStoreAsync`.
    3. If a store is disabled, its count is 0.
  - `PurgeStoreAsync(FormattableString batchSql)` loops:
    1. Run `n = await ExecuteSqlAsync(batchSql)`.
    2. Add `n` to the total.
    3. Stop when `n < BatchSize`.
  - The three parameterised batch statements (research R2/R3):
    - **Outbox**: `DELETE FROM payment_outbox WHERE "Id" IN (SELECT "Id" FROM payment_outbox WHERE "Status" = 'Published' AND "PublishedAt" < {cutoff} ORDER BY "PublishedAt" LIMIT {batch} FOR UPDATE SKIP LOCKED)`.
    - **Dead letters**: `DELETE FROM consumed_dead_letters WHERE "Id" IN (SELECT "Id" … WHERE "FailedAt" < {cutoff} ORDER BY "FailedAt" LIMIT {batch} FOR UPDATE SKIP LOCKED)`.
    - **Markers**: `DELETE FROM order_cancellation_markers WHERE "OrderId" IN (SELECT "OrderId" … WHERE "RecordedAt" < {cutoff} ORDER BY "RecordedAt" LIMIT {batch} FOR UPDATE SKIP LOCKED)`.
  - Write the outbox status as `nameof(OutboxStatus.Published)`, passed as a parameter.
  - Add an XML doc citing Constitution VI "Data retention" and ADR 0007.
  - Create `src/Ecommerce.Payments.Infrastructure/Retention/RetentionServiceCollectionExtensions.cs` with `AddRetention()`, which registers `IRetentionPurger` as scoped and does a `TryAddSingleton(TimeProvider.System)`.

  Depends on T001 and T002.

**Checkpoint**: the purger is built and indexed, but no worker runs it yet.

---

## Phase 2: User Story 1 — Old published announcements are purged (P1) 🎯 MVP

**Goal**: delete only published outbox entries that are older than the retention period.

**Independent Test**: on a real database, run `PurgeAsync` and check that only published entries
past the cutoff are gone.

- [ ] T005 [P] [US1] Write integration tests **(new)** in `tests/Ecommerce.Payments.Integration.Tests/RetentionPurgerTests.cs`. Use a PostgreSQL container with migrations applied, a `FakeTimeProvider` that starts on whole seconds, and helpers that insert rows directly with chosen timestamps.
  - `Purges_Only_Published_Outbox_Entries_Older_Than_Retention`: rows are published 31 days ago (deleted), published 29 days ago (kept), `Pending` 400 days old (kept), and `Exhausted` 400 days old (kept). The test asserts `PurgeResult.OutboxEntries == 1`.
  - `Outbox_Retention_Zero_Purges_Nothing`.
  - `Purging_Published_Earlier_Entry_Keeps_Later_Pending_Entry_Claimable`: payment P has #1 `Published` 31 days ago and #2 `Pending` and due. After the purge, `PaymentOutboxRepository.ClaimDueAsync` returns #2.
  - `Migration_Creates_Retention_Indexes`: the three index names exist in `pg_indexes`, and the outbox index is partial.

  These tests pass against T004. The task's value is proving the behaviour.

**Checkpoint**: outbox retention is proven on a real database.

---

## Phase 3: User Story 2 — Old dead letters and cancellation markers are purged (P2)

**Goal**: apply the same age-based purge to `consumed_dead_letters` and `order_cancellation_markers`, each with its own setting.

**Independent Test**: old rows are deleted and recent rows are kept; a store set to 0 is untouched.

- [ ] T006 [US2] Extend `tests/Ecommerce.Payments.Integration.Tests/RetentionPurgerTests.cs` with:
  - `Purges_Dead_Letters_Older_Than_Retention`: one stored 91 days ago is deleted, one stored 89 days ago is kept.
  - `Purges_Markers_Older_Than_Retention`: one recorded 181 days ago is deleted, one recorded 179 days ago is kept.
  - `Each_Store_Uses_Its_Own_Retention`: with outbox at 0, dead letters at 1 and markers at 0, only the old dead letter is deleted.

**Checkpoint**: all three stores are purged according to their own settings.

---

## Phase 4: User Story 3 — Safe to run continuously, on any number of instances (P2)

**Goal**: drain large backlogs in batches, tolerate concurrent purgers, and run as a hosted worker that logs and retries.

**Independent Test**: a backlog of more than 10 batches clears in one call. Two concurrent purgers delete everything that is expired, with no errors. The worker runs on its interval and keeps going after a failure.

### Tests for User Story 3

- [ ] T007 [P] [US3] Extend `tests/Ecommerce.Payments.Integration.Tests/RetentionPurgerTests.cs` with:
  - `Purges_Backlog_Larger_Than_Ten_Batches_In_One_Call`: `BatchSize` = 10 and 105 expired dead letters, so one call returns 105 and leaves 0 rows.
  - `Concurrent_Purgers_Delete_Everything_Expired_Without_Errors`: two purgers, each with its own `DbContext`, run under `Task.WhenAll` against 500 expired markers and 50 recent ones, with `BatchSize` = 20. Their counts sum to 500, 50 rows remain, and neither throws.
- [ ] T008 [P] [US3] Write unit tests **(new)** in `tests/Ecommerce.Payments.Infrastructure.Tests/Retention/RetentionWorkerTests.cs`. Use a `FakeTimeProvider`, a fake `IRetentionPurger` that records calls and can throw, a DI scope factory, and `ListLogger`, following the existing `OutboxRelayWorkerTests`.
  - `Runs_Once_On_Start_Then_Every_Interval`.
  - `Logs_Error_And_Keeps_Running_When_A_Run_Fails`.
  - `Logs_Information_When_Something_Was_Deleted_And_Debug_Otherwise`.
  - `Stops_Cleanly_On_Cancellation`.

### Implementation for User Story 3

- [ ] T009 [US3] Create `RetentionWorker` **(new)** in `src/Ecommerce.Payments.Infrastructure/Retention/RetentionWorker.cs`, modelled on `OutboxRelayWorker`.
  - It is a `BackgroundService` whose constructor takes `IServiceScopeFactory`, `IOptions<RetentionOptions>`, `TimeProvider` and `ILogger<RetentionWorker>`.
  - `ExecuteAsync`:
    1. `await Task.Yield()`.
    2. Create a `PeriodicTimer(TimeSpan.FromMilliseconds(IntervalMs), timeProvider)`.
    3. In a do/while loop over the timer ticks, create a scope, resolve `IRetentionPurger`, and call `PurgeAsync`.
    4. Log one line: "Retention purge deleted {OutboxEntries} outbox entries, {DeadLetters} dead letters, {Markers} cancellation markers". Use Information when `Total > 0`, otherwise Debug.
    5. If the run throws something other than a stopping cancellation, log an Error and continue.
  - Write an XML doc for the class.

  Makes T008 pass.
- [ ] T010 [US3] Wire up the host.
  - In `src/Ecommerce.Payments.Consumer/Program.cs`, add `AddOptions<RetentionOptions>().Bind(section).Validate(o => o.IsValid(), "Retention settings: IntervalMs and BatchSize must be positive, retention days must be >= 0.").ValidateOnStart()`, then `AddRetention()` and `AddHostedService<RetentionWorker>()`.
  - Add the `Retention` section with its defaults to `src/Ecommerce.Payments.Consumer/appsettings.json`.

**Checkpoint**: the service purges by itself, safely, every hour.

---

## Phase 5: Polish & Cross-Cutting Concerns

- [ ] T011 [P] Write ADR **(new)** `docs/adr/0007-data-retention.md`.
  - **Context**: unbounded growth, and constitution v1.4.0.
  - **Decision**: purge by age with a setting per store, never purge `Pending` or `Exhausted` entries or business data, use batched `SKIP LOCKED` deletes, run as an hourly worker, and add age indexes.
  - **Rejected**: `pg_cron`, an unbounded `DELETE`, purging inside the relay, archiving.
  - **Consequences**: the marker trade-off, the dead-letter payload is lost after 90 days, a large first purge.
  - Add the ADR to `docs/adr/README.md`.
- [ ] T012 [P] Write `docs/retention.md` **(new)**: what gets purged and when, what is never purged, the settings table with defaults (0 = keep forever), the marker trade-off, advice to export or replay dead letters before they expire, a query to see what the next run would delete, and the log line.
  - In `docs/outbox.md`, replace "Published entries are kept … cleaning them up is a separate concern" with a link to retention.
  - In `docs/consumed-dead-letters.md`, replace "never removes them" with the 90-day retention and the same link.
- [ ] T013 [P] Update `README.md`:
  - **Background services table**: add `RetentionWorker`.
  - **Configuration table**: add the `Retention:*` keys.
  - **Known limitations**: remove "Published outbox entries are kept and never cleaned up", "Cancellation markers are never expired" and "the service never removes them" for dead letters. Add the marker trade-off.
  - **Project layout**: add `docs/retention.md`.
- [ ] T014 [P] Update the project READMEs and CLAUDE.md.
  - `src/Ecommerce.Payments.Infrastructure/README.md`: add the `Retention/` folder.
  - `src/Ecommerce.Payments.Consumer/README.md`: add the hosted service and its settings.
  - `CLAUDE.md`: replace the "in progress" retention note with the actual rule and settings.
- [ ] T015 Run the full quality gate and `docker build -t ecommerce-payments .`, then mark all tasks `[X]`.

---

## Dependencies & Execution Order

- **Phase 1**: T001 and T002 can run in parallel. T003 can run alongside them. T004 needs T001 and T002.
- **US1 (T005)** and **US2 (T006)** need T003 and T004. T006 extends the file created in T005, so it runs after it.
- **US3**: T007 extends the same file and runs after T006. T008 is independent and can run in parallel. T009 needs T001 and T002. T010 needs T009.
- **Polish**: T011 to T014 can start after T010. T015 comes last.

## Parallel Example: Phase 1

```text
T001 RetentionOptions (+tests)     T002 PurgeResult + IRetentionPurger     T003 indexes + migration
```

## Implementation Strategy

1. **Phase 1 + US1 (MVP)**: the outbox is the fastest-growing table. Commit as
   `feat(US-09): purge old published outbox entries`.
2. **US2**: commit as `test(US-09): retention for dead letters and markers`. The purger
   already covers these stores, so this phase adds tests.
3. **US3**: commit as `feat(US-09): hourly retention worker, batched and instance-safe`.
4. **Polish**: commit as `docs(US-09): …`, then run the gate and the Docker build.

The manual check is optional; quickstart.md describes it.
