---

description: "Task list for US-05 Do Not Charge an Already-Cancelled Order"
---

# Tasks: Do Not Charge an Already-Cancelled Order

**Input**: Design documents from `specs/005-cancellation-marker/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md,
contracts/order-events-handling.md and quickstart.md. All are present.

**Tests**: Included and REQUIRED. Constitution Principles IV and V require:
- unit tests for every Domain method and every Service handler;
- integration and component coverage.

quickstart.md maps each scenario to a test.

**Organization**: grouped by user story in spec.md:
- US1: record a cancellation marker (P1).
- US2: do not charge a marked order (P1).
- US3: safe under concurrency (P2).

**No Setup phase**: there are no new projects or packages.

**Quality gate**: after **every** task run, in order (Constitution VIII; Docker must be running):
1. `dotnet build`
2. `dotnet format --verify-no-changes`
3. `dotnet test`

**Commits**: Conventional Commits scoped `US-05`: `feat(US-05): …`, `test(US-05): …`,
`refactor(US-05): …` and `docs(US-05): …`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an incomplete task).
- **[Story]**: US1, US2 or US3.
- **(new)**: the file does not exist yet.

---

## Phase 1: Foundational (Blocking Prerequisites)

**Purpose**: everything the stories need, with no change in behaviour. This covers:
- the marker type and table;
- the marker repository;
- the per-order unit of work (transaction + advisory lock);
- moving both handlers onto the unit of work.

**⚠️ CRITICAL**: no user story work can begin until this phase is complete.

- [X] T001 [P] Write tests **(new)** in `tests/Ecommerce.Payments.Domain.Tests/Orders/CancellationMarkerTests.cs`:
  - `Record` keeps every value;
  - `Record` with an empty `orderId` throws `ArgumentException`;
  - `Record` with an empty `sourceEventId` throws `ArgumentException`;
  - a null `reason` is allowed;
  - `Rehydrate` restores every value without validation.
- [X] T002 Create `CancellationMarker` **(new)**, `public sealed class`, in `src/Ecommerce.Payments.Domain/Orders/CancellationMarker.cs`:
  - get-only `OrderId`, `SourceEventId`, `CancelledAt`, `RecordedAt` and `Reason` (`string?`);
  - `static Record(Guid orderId, Guid sourceEventId, DateTimeOffset cancelledAt, DateTimeOffset recordedAt, string? reason)`, which validates that the ids are not empty;
  - `static Rehydrate(...)`, documented as "persistence only";
  - XML docs that cite Constitution VI "Cancellation markers".

  Makes T001 pass.
- [X] T003 Create the Service ports **(new)**, each with XML docs:
  - `src/Ecommerce.Payments.Service/Orders/IOrderUnitOfWork.cs`: `Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken)`. Doc: opens a transaction that holds a per-order lock until it is committed or disposed; a second caller for the same order waits.
  - `src/Ecommerce.Payments.Service/Orders/IOrderTransaction.cs`: `IAsyncDisposable` with `Task CommitAsync(CancellationToken)`. Doc: disposing without a commit rolls back.
  - `src/Ecommerce.Payments.Service/Orders/ICancellationMarkerRepository.cs`: `Task<CancellationMarker?> GetByOrderIdAsync(Guid, CancellationToken)` and `Task AddAsync(CancellationMarker, CancellationToken)`.

  Depends on T002. After T002 it can run in parallel with T004 and T007.
- [X] T004 [P] Create `CancellationMarkerEntity` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/Entities/CancellationMarkerEntity.cs`:
  - properties `OrderId`, `SourceEventId`, `CancelledAt`, `RecordedAt`, `Reason` (`string?`);
  - an XML doc on every property.

  Then in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentsDbContext.cs`:
  - add `DbSet<CancellationMarkerEntity> CancellationMarkers`;
  - map it with `ToTable("order_cancellation_markers")` and `HasKey(m => m.OrderId)`;
  - update the class XML doc.
- [X] T005 Generate migration `AddOrderCancellationMarkers`:
  - run `dotnet ef migrations add AddOrderCancellationMarkers --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure --output-dir Persistence/Migrations`;
  - run `dotnet format` so it uses file-scoped namespaces like the existing migrations;
  - check that the migration only creates the table with PK `OrderId` and touches no existing table.

  Depends on T004.
- [X] T006 Implement `CancellationMarkerRepository` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/CancellationMarkerRepository.cs`:
  - map explicitly between the entity and `CancellationMarker`;
  - `GetByOrderIdAsync` uses `AsNoTracking()` and then `CancellationMarker.Rehydrate`;
  - `AddAsync` adds the entity and calls `SaveChangesAsync`.

  Depends on T002, T003, T004.
- [X] T007 [P] Create `PersistenceOptions` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/PersistenceOptions.cs`:
  - `SectionName = "Persistence"`;
  - `OrderLockTimeoutMs`, default 5000;
  - XML docs.

  Create `OrderLockKey` **(new)**, `internal static`, in `src/Ecommerce.Payments.Infrastructure/Persistence/OrderLockKey.cs`:
  - `long For(Guid orderId)` XORs the two 64-bit halves of `orderId.ToByteArray()`;
  - XML doc that cites research R2.
- [X] T008 Implement `OrderUnitOfWork` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/OrderUnitOfWork.cs`: `public sealed class OrderUnitOfWork : IOrderUnitOfWork` over the scoped `PaymentsDbContext` and `IOptions<PersistenceOptions>`.
  - `BeginAsync` runs, in order:
    1. `Database.BeginTransactionAsync`;
    2. `ExecuteSqlRawAsync("SET LOCAL lock_timeout = '<n>ms'")`, with `n` taken from the options as an `int`, so there is nothing to inject;
    3. `ExecuteSqlAsync($"SELECT pg_advisory_xact_lock({OrderLockKey.For(orderId)})")`.
  - It returns a private nested `OrderTransaction : IOrderTransaction`:
    - `CommitAsync` commits the transaction;
    - `DisposeAsync` without a commit rolls back, then calls `ChangeTracker.Clear()`, then disposes the transaction.
  - XML docs cite ADR 0004 and Constitution VI "Cancellation markers".

  Depends on T003, T007.
- [X] T009 Integration tests **(new)** in `tests/Ecommerce.Payments.Integration.Tests/OrderUnitOfWorkTests.cs`, on a PostgreSQL container with migrations applied:
  - `Second_Transaction_On_Same_Order_Waits_For_First_To_Commit`: tx1 begins for order X; tx2's `BeginAsync` for X has not completed after 300 ms; it completes after tx1 commits.
  - `Different_Orders_Do_Not_Wait`.
  - `Lock_Wait_Times_Out_With_Configured_Timeout`: `OrderLockTimeoutMs = 200`; tx2 throws a `PostgresException` with SqlState `55P03`.
  - `Dispose_Without_Commit_Rolls_Back_Marker_Insert`.
  - `Retry_After_Rollback_Starts_With_Clean_ChangeTracker`: inside a transaction, track a new
    `PaymentEntity` (`Add`, no save) and dispose without committing. Assert
    `ChangeTracker.Entries()` is empty. Then run a second `BeginAsync` that saves the same
    payment id and commits, and assert that it succeeds and leaves exactly one row (plan Risks).

  Also add `CancellationMarkerMigrationTests` **(new)** in `tests/Ecommerce.Payments.Integration.Tests/CancellationMarkerMigrationTests.cs`:
  - the table exists after migration;
  - inserting a second marker for the same `OrderId` raises a unique violation (SqlState `23505`).

  Depends on T005, T006, T008.
- [X] T010 Wire DI in `src/Ecommerce.Payments.Consumer/Program.cs`:
  - `AddOptions<PersistenceOptions>().Bind(section).Validate(o => o.OrderLockTimeoutMs > 0, ...).ValidateOnStart()`;
  - `AddScoped<IOrderUnitOfWork, OrderUnitOfWork>()`;
  - `AddScoped<ICancellationMarkerRepository, CancellationMarkerRepository>()`.

  Add `"Persistence": { "OrderLockTimeoutMs": 5000 }` to `src/Ecommerce.Payments.Consumer/appsettings.json`.

  Depends on T006, T008.
- [X] T011 Refactor `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` to take `IOrderUnitOfWork` and `ICancellationMarkerRepository`, keeping today's behaviour.
  - One `PersistenceRetry.Pipeline.ExecuteAsync` delegate per attempt does:
    1. `await using var tx = await _unitOfWork.BeginAsync(orderId, ct)`;
    2. `ExistsByOrderIdAsync`; true returns a "skip" result;
    3. `CreatePending` then `Evaluate`;
    4. `SaveAsync`;
    5. `tx.CommitAsync`;
    6. return the domain event.
  - Publishing stays after the delegate. The Error log and rethrow on persistent failure stay.
  - The marker repository is injected but not used yet; US2 uses it.
  - Update every test that builds or registers this handler to supply the new dependencies:
    - `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`: a Moq `IOrderUnitOfWork` returning a mock `IOrderTransaction`, plus a marker-repository mock returning null. Add an assertion that `CommitAsync` is called before the publish.
    - The integration tests `OrderPlacedToPaymentTests.cs`, `OrderPlacedToFailedPaymentTests.cs` and `TransientSaveFailureTests.cs`: register the real `OrderUnitOfWork`, `CancellationMarkerRepository` and `Options.Create(new PersistenceOptions())`.
    - The component tests `ProcessPaymentFlowTests.cs`, `ProcessPaymentFailureFlowTests.cs`, `DuplicateDeliveryTests.cs`, `DuplicateFailedDeliveryTests.cs`, `PublishFailureDeadLetterTests.cs` and `DeadLetterRetryFlowTests.cs`: the same three registrations.

  All existing assertions must stay green. Depends on T010.
- [X] T012 Refactor `src/Ecommerce.Payments.Service/Payments/RefundPaymentHandler.cs` the same way, keeping US-04 behaviour:
  - each attempt runs inside `BeginAsync(orderId)`;
  - the load, `eventId` dedupe, `RequestRefund` and `SaveRefundAsync` run inside the transaction;
  - `CommitAsync` runs after a successful `SaveRefundAsync`;
  - the delegate returns a private result (`Refunded(event)` or one of the ignore reasons);
  - logging and `PublishRefundedAsync` run after the delegate.

  Add an `ICancellationMarkerRepository` constructor parameter, unused yet. Update `tests/Ecommerce.Payments.Service.Tests/Payments/RefundPaymentHandlerTests.cs`, `tests/Ecommerce.Payments.Integration.Tests/OrderCancelledToRefundTests.cs` and `tests/Ecommerce.Payments.Component.Tests/RefundFlowTests.cs` with the new dependencies; all assertions unchanged. Depends on T010.

**Checkpoint**:
- The marker table exists.
- The per-order lock works and is proven by T009.
- Both handlers run inside it with unchanged behaviour.
- The quality gate is green.

---

## Phase 2: User Story 1 — Record a cancellation that arrives before the payment (P1) 🎯 MVP

**Goal**: an `OrderCancelled` for an order with no payment records one marker and logs it. A
redelivery, or a different cancellation for the same order, changes nothing and is logged.

**Independent Test**: deliver an `OrderCancelled` for an unknown order. Then check:
- one marker row exists with the event's id, time and reason;
- there is an Information log;
- nothing is published;
- the message is committed.

### Tests for User Story 1

- [X] T013 [P] [US1] Add unit tests to `tests/Ecommerce.Payments.Service.Tests/Payments/RefundPaymentHandlerTests.cs`, using a `FakeTimeProvider` and the inner `ListLogger`:
  - `Should_Record_Marker_When_No_Payment_Exists`: `AddAsync` is called with orderId, eventId, `CancelledAt = occurredAt`, `RecordedAt = clock` and reason; the commit happens; no publish; one Information log containing "recorded".
  - `Should_Not_Add_Marker_When_Marker_With_Same_EventId_Exists`: no add, no commit-dependent write, no publish, an Information log "duplicate delivery".
  - `Should_Not_Add_Marker_When_Marker_With_Other_EventId_Exists`: no add, an Information log "already cancelled".
  - `Should_Throw_And_Not_Publish_When_Marker_Save_Keeps_Failing`: `AddAsync` throws every time; an Error log; rethrows; no publish.

  Replace the US-04 test that expected "no payment exists" to be ignored.
- [X] T014 [P] [US1] Integration tests **(new)** in `tests/Ecommerce.Payments.Integration.Tests/CancellationBeforePaymentTests.cs`, mirroring `OrderCancelledToRefundTests.cs` (drive `OrderCancelledConsumer.HandleMessageAsync` against PostgreSQL):
  - `OrderCancelled_Without_Payment_Records_One_Marker`.
  - `Redelivered_OrderCancelled_Leaves_One_Marker`.
  - `Second_OrderCancelled_With_New_EventId_Leaves_Original_Marker`: the stored `SourceEventId` is still the first one.
  - `Malformed_OrderCancelled_Records_No_Marker`.

### Implementation for User Story 1

- [X] T015 [US1] Implement the no-payment branch in `src/Ecommerce.Payments.Service/Payments/RefundPaymentHandler.cs`, inside the transaction. Look up the marker with `GetByOrderIdAsync` and handle three cases:
  - **No marker**: `CancellationMarker.Record(orderId, eventId, occurredAt, _timeProvider.GetUtcNow(), reason)`, then `AddAsync`, then `CommitAsync`, returning `MarkerRecorded`.
  - **Same `SourceEventId`**: return `DuplicateMarker`.
  - **Other `SourceEventId`**: return `AlreadyMarked`.

  After the delegate, log one Information line per result:
  - `MarkerRecorded`: "Recorded cancellation of order {OrderId} from OrderCancelled {EventId} before any payment exists; a later OrderPlaced will not be charged (reason: {Reason})."
  - `DuplicateMarker`: "Ignoring OrderCancelled {EventId} for order {OrderId}: duplicate delivery; cancellation already recorded."
  - `AlreadyMarked`: "Ignoring OrderCancelled {EventId} for order {OrderId}: order already recorded as cancelled by {SourceEventId}."

  Update the class XML doc. Makes T013 and T014 pass.

**Checkpoint**: early cancellations are remembered. US-04 refund behaviour is unchanged.

---

## Phase 3: User Story 2 — Do not charge an order that is already cancelled (P1)

**Goal**: an `OrderPlaced` for a marked order creates no payment and publishes nothing, and it
is logged. Orders without a marker are unchanged.

**Independent Test**: record a marker, then deliver the `OrderPlaced`. Then check:
- no `payments` row;
- nothing on `payments.payment-processed` or `payments.payment-failed`;
- an Information log;
- the message is committed.

### Tests for User Story 2

- [X] T016 [P] [US2] Add unit tests to `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`:
  - `Should_Not_Create_Or_Publish_Payment_When_Order_Has_Cancellation_Marker`: no `SaveAsync`, no `PublishAsync`, no `PublishFailedAsync`, and one Information log naming the order and the marker's `SourceEventId`. Check the existing tests for how the logger is captured; add a list logger if there is none.
  - `Should_Check_Payment_Before_Marker`: when a payment exists, the marker repository is not queried.
  - `Should_Process_Normally_When_No_Marker`: the existing happy path, with the marker mock returning null.
- [X] T017 [P] [US2] Extend `tests/Ecommerce.Payments.Integration.Tests/CancellationBeforePaymentTests.cs`:
  - `OrderPlaced_After_Marker_Creates_No_Payment_And_Publishes_Nothing`: uses `FakePaymentEventPublisher` and checks that nothing was recorded.
  - `Redelivered_OrderPlaced_After_Marker_Still_Creates_Nothing`.
  - `OrderPlaced_Then_OrderCancelled_Refunds_And_Records_No_Marker`: a below-threshold amount gives Refunded; no marker row.
  - `Failed_Payment_Then_OrderCancelled_Records_No_Marker`.
- [X] T018 [P] [US2] Component test **(new)** in `tests/Ecommerce.Payments.Component.Tests/CancelBeforePlaceFlowTests.cs`, with PostgreSQL and Kafka containers and both consumers hosted, set up like `RefundFlowTests.cs`:
  - `Cancel_Then_Place_Publishes_Nothing_And_Charges_Nothing`:
    1. Produce `OrderCancelled` for X and wait for the marker row.
    2. Produce `OrderPlaced` for X and note its delivery offset from the produce result.
    3. Poll the consumer group's committed offset for that partition of `orders.order-placed`
       until it is greater than the noted offset (timeout 30 s). Read it with a separate
       `IConsumer` in the same group via `Committed(...)`, without subscribing.
    4. Assert there is no `payments` row for X, and that a `KafkaTestConsumer` reading the
       processed, failed and refunded topics from the beginning sees no message with
       `orderId` = X within 5 s.

### Implementation for User Story 2

- [X] T019 [US2] In `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs`, inside the transaction and after the payment-exists check, call `ICancellationMarkerRepository.GetByOrderIdAsync(orderId)`.
  - If a marker is found, return a `SkipCancelled(marker.SourceEventId)` result without calling Domain or saving.
  - After the delegate, log one Information line: "Not charging order {OrderId} (OrderPlaced {EventId}): the order was already cancelled by OrderCancelled {CancelEventId}; no payment created and nothing published."
  - Update the class XML doc to cite Constitution VI "Cancellation markers".

  Makes T016 to T018 pass.

**Checkpoint**: cancel-before-place no longer charges the customer. Both P1 stories are done.

---

## Phase 4: User Story 3 — Safe when both notifications arrive at the same time (P2)

**Goal**: concurrent `OrderPlaced` and `OrderCancelled` for one order always end in an allowed
outcome. The guarantee comes from the lock added in Phase 1; this phase proves it.

**Independent Test**: run 50 concurrent rounds and check that every order ends in one of these
states:
- a marker and no payment;
- Refunded and no marker;
- Failed and no marker.

### Tests for User Story 3

- [X] T020 [US3] Integration tests **(new)** in `tests/Ecommerce.Payments.Integration.Tests/PlaceCancelRaceTests.cs`, on a PostgreSQL container. Each handler runs in its own DI scope and `DbContext`, released together by a `Barrier` or a shared `TaskCompletionSource`.
  - `Concurrent_Place_And_Cancel_Always_End_In_Allowed_Outcome`: 50 rounds, new order ids, amounts alternating below and above the threshold. Per order, assert:
    - never both a marker and a payment;
    - never a `Processed` payment;
    - at most one `PublishedRefunds` entry, and it matches a Refunded payment;
    - no processed or failed event for an order with a marker.
  - `Place_Wins_When_Committed_First_Then_Cancel_Refunds`: forced ordering, using a test
    `GatedOrderUnitOfWork : IOrderUnitOfWork` that wraps the real `OrderUnitOfWork`.
    - Its `BeginAsync` calls the inner one, then sets a `TaskCompletionSource` called
      `LockAcquired`.
    - The returned transaction's `CommitAsync` first awaits a `ReleaseCommit` gate.
    - Run the place handler with the gated unit of work. Await `LockAcquired`, then start the
      cancel handler with the real unit of work.
    - Assert the cancel handler has not finished after 300 ms. Release the gate, then assert
      that the cancel handler waited and refunded, and that there is no marker.
  - `Cancel_Wins_When_Committed_First_Then_Place_Creates_Nothing`: the same arrangement with
    the gated unit of work on the cancel handler. Assert there is a marker, no payment and
    nothing published.
  - `Without_Order_Lock_The_Race_Can_Leave_Processed_Payment_With_Marker`: swap in a no-op `IOrderUnitOfWork` that only opens a transaction. Run up to 50 rounds with a short synchronisation delay after each handler's reads (inject via a delaying repository decorator). Assert at least one round ends with both a marker and a payment. This documents why the lock is needed (research R9). If it proves flaky on CI, mark it `[Trait("Category", "Demonstration")]` and say so in the test's XML doc.

### Implementation for User Story 3

- [X] T021 [US3] Fix whatever T020 exposes; no code change is expected. Confirm the following and keep them:
  - the Information log for losing the race comes from the existing branches (FR-007: US-04's "refunded concurrently" or US1's markers);
  - no Error-level log appears for a lost race in `PlaceCancelRaceTests`; assert this through a capturing logger provider.

**Checkpoint**: FR-006, FR-007 and SC-002 are proven on a real database.

---

## Phase 5: Polish & Cross-Cutting Concerns

- [ ] T022 [P] Write ADR **(new)** `docs/adr/0004-per-order-serialisation.md` from `docs/adr/template.md`:
  - Context: the race, as in research R1.
  - Decision: marker table plus `pg_advisory_xact_lock` as the first statement, plus the lock timeout.
  - Rejected alternatives: SERIALIZABLE, re-check after commit, row lock, registry table.
  - Consequences: markers kept forever; every handler that touches an order's state takes the lock first.
  - Reference `ecommerce-inventory` ADR 0005.

  Add it to `docs/adr/README.md`.
- [ ] T023 [P] Update `README.md`:
  - remove the "Known limitations" entry about `OrderCancelled` arriving before its payment;
  - point the "no inbound dead-letter store" entry at the planned follow-up US-06;
  - describe the cancellation marker and the per-order lock in the event-flow or refunds section;
  - add `Persistence:OrderLockTimeoutMs` to the configuration table;
  - mention the new table.
- [ ] T024 [P] Update the project READMEs:
  - `src/Ecommerce.Payments.Domain/README.md`: `CancellationMarker`.
  - `src/Ecommerce.Payments.Service/README.md`: the unit of work, the marker branches of both handlers and the `Orders/` ports.
  - `src/Ecommerce.Payments.Infrastructure/README.md`: the table, repository, `OrderUnitOfWork` and options.
  - `src/Ecommerce.Payments.Consumer/README.md`: the new setting.
- [ ] T025 [P] Update `CLAUDE.md`:
  - every handler that reads or changes an order's state runs inside `IOrderUnitOfWork.BeginAsync(orderId)` (lock first) and publishes only after commit;
  - wrap the whole unit in `PersistenceRetry`;
  - the new setting;
  - ADR 0004.
- [ ] T026 Run the full quality gate (`dotnet build`, `dotnet format --verify-no-changes`, `dotnet test`) and `docker build -t ecommerce-payments .`. Mark all tasks `[X]`.
- [ ] T027 Manual check from `specs/005-cancellation-marker/quickstart.md` ("Manual check") against `ecommerce-infra`. Do it **only** if no other local Payments Consumer is running; otherwise skip it and record the reason here.

---

## Dependencies & Execution Order

### Phase dependencies

- **Foundational (Phase 1)** blocks every user story.
- **US1 (Phase 2)** depends on Phase 1.
- **US2 (Phase 3)** depends on Phase 1. Its tests need markers, which they can insert directly through the repository, so US2 does not strictly need US1. T018's end-to-end check, which produces the cancellation through Kafka, needs US1.
- **US3 (Phase 4)** depends on US1 and US2: both sides must be implemented for the race test to be meaningful.
- **Polish (Phase 5)** comes after the stories. T022 to T025 can start any time after Phase 1.

### Within Phase 1

```
T001 → T002 → T003 ─┬→ T006 ─┐
T004 → T005 ────────┘        ├→ T009
T007 → T008 ─────────────────┤
                              └→ T010 → T011, T012 (same files as later stories → sequential)
```

### Within each story

Tests (T013/T014, T016 to T018, T020) are written first and must fail before the implementation
(T015, T019, T021).

## Parallel Example: Phase 1

```text
T001 CancellationMarkerTests        T004 CancellationMarkerEntity + DbContext      T007 PersistenceOptions + OrderLockKey
```

## Parallel Example: User Story 2

```text
T016 ProcessPaymentHandlerTests     T017 CancellationBeforePaymentTests (US2 cases)     T018 CancelBeforePlaceFlowTests
```

## Implementation Strategy

1. **Phase 1**: no behaviour change; it is guarded by the full existing suite. Commit as
   `refactor(US-05): per-order unit of work and cancellation marker groundwork`.
2. **US1 + US2 (MVP)**: together these remove the "charged after cancel" bug in the sequential
   case. Commit per story with `feat(US-05)`.
3. **US3**: proves the concurrent case. Commit as `test(US-05)`.
4. **Polish**: commit as `docs(US-05)`. Run the gate and the Docker build.

## Notes

- Never publish inside the transaction: publishing happens only after `CommitAsync` returns
  (Constitution II).
- Every handler that touches an order's state takes the lock as the first statement of its
  transaction (ADR 0004).
- The quality gate runs after every task. If the local Consumer is locking `bin/Debug`, run the
  gate with `-c Release`.
