---

description: "Task list template for feature implementation"
---

# Tasks: Handle Payment Failure

**Input**: Design documents from `specs/002-handle-payment-failure/`

**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/,
quickstart.md (all present)

**Tests**: Included and REQUIRED — same rationale as US-01: Constitution Principles IV and V
mandate unit tests for every Domain method and Service handler, plus integration and component
coverage.

**Organization**: Tasks are grouped by user story (from spec.md) to enable independent
implementation and testing of each story.

**No separate Setup phase**: unlike US-01, this feature adds no new projects — it extends the
existing solution in place. Numbering starts directly at Foundational.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2)
- Paths are existing US-01 files being extended unless marked **(new)**

---

## Phase 1: Foundational (Blocking Prerequisites)

**Purpose**: Shared low-level building blocks — new types, config surface, and schema changes —
that User Story 1 assembles into working behavior. User Story 2 needs none of its own; it only
proves an existing US-01 mechanism (idempotency) already covers the new failure path.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T001 [P] Add the `PaymentFailed` domain event **(new)**, mirroring `PaymentProcessed` (`PaymentId`, `OrderId`, `Reason`, `FailedAt`), in `src/Ecommerce.Payments.Domain/Payments/PaymentFailed.cs`
- [X] T002 [P] Add `FailureReason` (string?) and `FailedAt` (DateTimeOffset?) properties (private setters, null by default) to the `Payment` aggregate shell in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` (the `Fail()`/`Evaluate()` behavior is added in US1, T018/T019)
- [X] T003 [P] Add the `PaymentFailedEvent` DTO **(new)**, mirroring `PaymentProcessedEvent` (`eventId`, `occurredAt`, `aggregateId`, `version`, `orderId`, `reason`, `failedAt`), in `src/Ecommerce.Payments.Infrastructure/Messaging/IntegrationEvents/PaymentFailedEvent.cs` per [contracts/payments.payment-failed.md](./contracts/payments.payment-failed.md)
- [X] T004 [P] Add a `PaymentFailedTopic` property to `KafkaOptions` in `src/Ecommerce.Payments.Infrastructure/Messaging/KafkaOptions.cs`
- [X] T005 [P] Add `PaymentPolicyOptions` **(new)** with a `MaxAmountThreshold` (decimal) property in `src/Ecommerce.Payments.Service/Payments/PaymentPolicyOptions.cs`
- [X] T006 [P] Extend `IPaymentEventPublisher` with `PublishFailedAsync(PaymentFailed paymentFailed, CancellationToken cancellationToken)` in `src/Ecommerce.Payments.Service/Payments/IPaymentEventPublisher.cs` (temporary `NotImplementedException` stub added to `PaymentEventPublisher`/`FakePaymentEventPublisher` to keep the build green until T020)
- [X] T007 [P] Add `FailureReason` (nullable text) and `FailedAt` (nullable timestamptz) properties to `PaymentEntity` in `src/Ecommerce.Payments.Infrastructure/Persistence/Entities/PaymentEntity.cs` (no `PaymentsDbContext` mapping changes needed — both map by EF Core convention, unlike `Status`, which already has an explicit conversion)
- [X] T008 Generate the `AddPaymentFailureFields` EF Core migration adding the two new nullable columns to `payments` in `src/Ecommerce.Payments.Infrastructure/Persistence/Migrations/` (depends on T007)
- [X] T009 [P] Add `Kafka:PaymentFailedTopic` (`payments.payment-failed`) and a `PaymentPolicy:MaxAmountThreshold` default (`10000.00`) to `src/Ecommerce.Payments.Consumer/appsettings.json`

**Also fixed during this phase**: discovered `core.autocrlf=true` with no `.gitattributes` was
causing every file to be checked out with CRLF endings, failing `dotnet format --verify-no-changes`
against `.editorconfig`'s `end_of_line = lf` — added `.gitattributes` (`* text=auto eol=lf`),
ran `dotnet format` to normalize the working tree, and `git add --renormalize .` to fix the
index. This was a repo-wide issue, not specific to this feature, and would have hit any fresh
checkout on a Windows machine.

**Checkpoint**: New types, schema, and config surface exist — User Story 1 implementation can
now begin. Run the build/format/test gate before continuing.

---

## Phase 2: User Story 1 - Record and announce a payment that cannot be completed (Priority: P1) 🎯 MVP

**Goal**: An `OrderPlaced` message whose amount exceeds the configured threshold results in a
`Payment` recorded as `Failed` (with a reason), durably saved, and a `PaymentFailed` event
published afterward — under the same ordering guarantee as the success path. An order at or
below the threshold still succeeds exactly as before (US-01 unchanged).

**Independent Test**: Send one `OrderPlaced` message whose `totalAmount` exceeds the configured
threshold (quickstart.md Scenario 1) and confirm a `Failed` `Payment` row with a reason exists
and a matching `PaymentFailed` message was published — and confirm (Scenario 3) a below-threshold
order still produces a `Processed` payment as before.

### Tests for User Story 1

- [X] T010 [P] [US1] Unit test: `Payment.Fail(reason)` happy path transitions `Pending → Failed`, sets `FailureReason`/`FailedAt`, and raises `PaymentFailed`, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs`
- [X] T011 [P] [US1] Unit test: `Payment.Fail(reason)` on an already-terminal payment (`Processed` or `Failed`) throws `InvalidPaymentTransitionException` without mutating state, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs`
- [X] T012 [P] [US1] Unit test: `Payment.Evaluate(maxAmountThreshold)` with `Amount` at or below the threshold transitions to `Processed` and returns a `PaymentProcessed`, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs`
- [X] T013 [P] [US1] Unit test: `Payment.Evaluate(maxAmountThreshold)` with `Amount` above the threshold transitions to `Failed`, with a reason referencing the threshold, and returns a `PaymentFailed`, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs`
- [X] T014 [P] [US1] Unit test: `ProcessPaymentHandler` calls `IPaymentEventPublisher.PublishFailedAsync` (never `PublishAsync`) when `Evaluate()` returns a `PaymentFailed`, and never logs at `Error` severity for this case, using Moq, in `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`
- [X] T015 [P] [US1] Unit test (regression): `ProcessPaymentHandler` still calls `IPaymentEventPublisher.PublishAsync` (never `PublishFailedAsync`) when `Evaluate()` returns a `PaymentProcessed`, in `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`
- [X] T016 [P] [US1] Integration test: an `OrderPlaced` message with `totalAmount` above the threshold results in a `Payment` row with `Status = Failed` and `FailureReason` populated, against containerized PostgreSQL, in `tests/Ecommerce.Payments.Integration.Tests/OrderPlacedToFailedPaymentTests.cs` — verified passing against real Docker/PostgreSQL
- [X] T017 [US1] Component test: full flow with containerized Kafka + PostgreSQL — consume an above-threshold `OrderPlaced` → `Payment` saved `Failed` → `PaymentFailed` published to `payments.payment-failed` with the correct payload, in `tests/Ecommerce.Payments.Component.Tests/ProcessPaymentFailureFlowTests.cs` — verified passing against real Docker/Kafka/PostgreSQL

### Implementation for User Story 1

- [X] T018 [US1] Implement `Payment.Fail(string reason)` — transitions `Pending → Failed`, sets `FailedAt`/`FailureReason`, raises `PaymentFailed`; throws `InvalidPaymentTransitionException` if not currently `Pending` — in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` (depends on T002; makes T010/T011 pass)
- [X] T019 [US1] Implement `Payment.Evaluate(decimal maxAmountThreshold)` — the sole place the amount-vs-threshold business rule is evaluated; calls `Process()` or `Fail(reason)` internally and returns the resulting `PaymentDomainEvent` — in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` (depends on T018; makes T012/T013 pass)
- [X] T020 [US1] Implement `PaymentEventPublisher.PublishFailedAsync` — Confluent.Kafka producer to `Kafka:PaymentFailedTopic`, with the same dead-letter fallback on publish failure as `PublishAsync` (generalized `WriteDeadLetterAsync` to take `paymentId`/`eventType` instead of being `PaymentProcessed`-specific) — in `src/Ecommerce.Payments.Infrastructure/Messaging/PaymentEventPublisher.cs` (depends on T003, T004, T006)
- [X] T021 [US1] Update `ProcessPaymentHandler` to accept `IOptions<PaymentPolicyOptions>`, call `Payment.Evaluate(maxAmountThreshold)` instead of `Process()`, and route the resulting event by type (`PaymentProcessed` → `PublishAsync`, `PaymentFailed` → `PublishFailedAsync`) — Service never inspects `Amount` itself — in `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` (depends on T019, T020, T005; makes T014/T015 pass). **Bug found and fixed during this task**: `PaymentPolicyOptions.MaxAmountThreshold` must default to `decimal.MaxValue`, not `0` — an unconfigured/missing `PaymentPolicy` section would otherwise silently fail *every* payment (caught by 3 regressing US-01 tests that don't configure the option).
- [X] T022 [US1] Update `PaymentRepository.SaveAsync` to map `FailureReason`/`FailedAt` from the `Payment` aggregate onto `PaymentEntity` in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs` (depends on T007, T008)
- [X] T023 [US1] Register `PaymentPolicyOptions` binding (from the `PaymentPolicy` configuration section) in DI in `src/Ecommerce.Payments.Consumer/Program.cs` (depends on T005, T009). Also required adding the `Microsoft.Extensions.Options` package to both `Ecommerce.Payments.Service` and `Ecommerce.Payments.Service.Tests` (not previously referenced).

**Checkpoint**: User Story 1 is fully functional and independently testable — this is the MVP.
Run the build/format/test gate before continuing.

---

## Phase 3: User Story 2 - Never double-announce a payment failure on duplicate notifications (Priority: P2)

**Goal**: Redelivery of an `OrderPlaced` message that already resulted in a recorded payment
failure is a no-op — no second `Failed` `Payment` row, no second `PaymentFailed` publish.

**Independent Test**: Send the identical above-threshold `OrderPlaced` message twice
(quickstart.md Scenario 2) and confirm only one `Failed` `Payment` row and one `PaymentFailed`
publish exist.

**No new implementation**: `ProcessPaymentHandler`'s existing `ExistsByOrderIdAsync` check
(built in US-01) already gates on "does *any* `Payment` row exist for this order," regardless of
status, and already runs before `Evaluate()` is ever called. This story only needs test coverage
proving that guarantee extends to the failure path — see research.md #3.

### Tests for User Story 2

- [X] T024 [US2] Component test: redelivering the identical above-threshold `OrderPlaced` message twice results in exactly one `Failed` `Payment` row and exactly one `PaymentFailed` publish, in `tests/Ecommerce.Payments.Component.Tests/DuplicateFailedDeliveryTests.cs` — verified passing against real Docker/Kafka/PostgreSQL, confirming zero new production code was needed

**Checkpoint**: User Stories 1 AND 2 both work independently. Run the build/format/test gate
before continuing.

---

## Final Phase: Polish & Cross-Cutting Concerns

- [X] T025 [P] Add XML doc comments to `Payment.Fail()`, `Payment.Evaluate()`, `PaymentFailed`, `PaymentFailedEvent`, and `PaymentPolicyOptions` describing every member, per Constitution Documentation Standards — already complete, added inline while writing each member in T001-T021
- [X] T026 Run `dotnet build`, `dotnet format --verify-no-changes`, and `dotnet test` for the full solution as a final confirmation (in addition to, not instead of, running the same gate after every preceding task); fix any failure before considering the feature done, per Constitution Principle VIII — 38/38 tests passing solution-wide, verified across multiple repeated runs. **Flaky test found and fixed during this task**: `ProcessPaymentFailureFlowTests` intermittently failed with `ConsumeException: Unknown topic or partition` — Testcontainers Kafka component test classes run in parallel, and a brand-new assertion-side consumer's first metadata fetch can race a topic's auto-creation. Added a shared `KafkaTestConsumer.SubscribeAndConsume` retry helper and applied it (plus inline retry-on-`UnknownTopicOrPart`) across all four component test files that consume a topic right after producing to it, including the two from US-01.
- [X] T027 Execute [quickstart.md](./quickstart.md) validation Scenarios 1-3 manually — ran the real `Ecommerce.Payments.Consumer` host against the shared `ecommerce-infra` Kafka + payments PostgreSQL with `PaymentPolicy__MaxAmountThreshold=100`, applied the new migration, created the `payments.payment-failed` topic, and produced real messages via `kafka-console-producer`; all 3 scenarios confirmed (above-threshold → Failed + published; redelivery → no duplicate; below-threshold → still Processed as in US-01), test data cleaned up afterward

---

## Dependencies & Execution Order

### Phase Dependencies

- **Foundational (Phase 1)**: No dependencies on this feature's own prior work (but the whole
  feature depends on US-01 already being merged) — BLOCKS both user stories
- **User Story 1 (Phase 2)**: Depends on Foundational only
- **User Story 2 (Phase 3)**: Depends on Foundational + User Story 1's `ProcessPaymentHandler`
  changes (T021) being in place — it is a test-only story that exercises code User Story 1 built
- **Final Phase**: Depends on both user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: No dependencies on other stories — independently testable as-is (the MVP)
- **User Story 2 (P2)**: Requires User Story 1's implementation to exist (there is no separate
  production code for US2), but is independently *testable* via its own scenario (duplicate
  delivery of a failing order produces no extra side effects)

### Within Each User Story

- Tests are written first and MUST fail before the corresponding implementation task
- Domain (`Fail()` before `Evaluate()`) before Infrastructure (`PublishFailedAsync`) before
  Service (`ProcessPaymentHandler` routing) before Consumer wiring (DI registration)
- Story complete and checkpoint validated before moving to the next priority

### Parallel Opportunities

- All Foundational tasks marked [P] (T001-T007, T009) run in parallel; T008 (migration) depends
  on T007 and runs after it
- All US1 test tasks marked [P] (T010-T016) run in parallel with each other (not with T017,
  which needs the full stack)
- T024 (US2's only task) can be written as soon as US1 is checkpointed

---

## Parallel Example: Foundational Phase

```bash
Task: "Add PaymentFailed domain event in src/Ecommerce.Payments.Domain/Payments/PaymentFailed.cs"
Task: "Add FailureReason/FailedAt properties to Payment in src/Ecommerce.Payments.Domain/Payments/Payment.cs"
Task: "Add PaymentFailedEvent DTO in src/Ecommerce.Payments.Infrastructure/Messaging/IntegrationEvents/PaymentFailedEvent.cs"
Task: "Add KafkaOptions.PaymentFailedTopic in src/Ecommerce.Payments.Infrastructure/Messaging/KafkaOptions.cs"
Task: "Add PaymentPolicyOptions in src/Ecommerce.Payments.Service/Payments/PaymentPolicyOptions.cs"
Task: "Extend IPaymentEventPublisher with PublishFailedAsync in src/Ecommerce.Payments.Service/Payments/IPaymentEventPublisher.cs"
Task: "Add FailureReason/FailedAt to PaymentEntity in src/Ecommerce.Payments.Infrastructure/Persistence/Entities/PaymentEntity.cs"
```

## Parallel Example: User Story 1 Tests

```bash
Task: "Unit test Payment.Fail() happy path in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test Payment.Fail() invalid transition in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test Payment.Evaluate() below threshold in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test Payment.Evaluate() above threshold in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test ProcessPaymentHandler routes PaymentFailed in tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs"
Task: "Unit test ProcessPaymentHandler routes PaymentProcessed (regression) in tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs"
Task: "Integration test Consumer->Repository for a failed payment in tests/Ecommerce.Payments.Integration.Tests/OrderPlacedToFailedPaymentTests.cs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Foundational (blocks everything else)
2. Complete Phase 2: User Story 1
3. **STOP and VALIDATE**: run quickstart.md Scenarios 1 and 3 end-to-end (failure path + success-path regression)
4. This is a deployable increment — payments that exceed the threshold are now recorded and
   announced instead of silently disappearing

### Incremental Delivery

1. Foundational → shared types/schema ready
2. Add User Story 1 → validate via Scenarios 1 + 3 → MVP
3. Add User Story 2 → validate via Scenario 2 (duplicate protection)
4. Polish → full constitution compliance (build/format/test gate, docs)

---

## Notes

- [P] tasks touch different files with no incomplete dependencies
- [Story] labels map tasks to spec.md's user stories for traceability
- `Payment.Fail()` (T018) and `Payment.Evaluate()` (T019) are split into two Domain tasks/tests
  on purpose, mirroring US-01's `CreatePending()`/`Process()` split — `Fail()` is a pure state
  transition primitive (independently testable), `Evaluate()` is the one place the business rule
  lives (Constitution Principle I)
- `ProcessPaymentHandler.cs`, `PaymentEntity.cs`, `KafkaOptions.cs`, and `IPaymentEventPublisher.cs`
  are all existing US-01 files being extended, not replaced — see plan.md's Project Structure
- **Run `dotnet build` → `dotnet format --verify-no-changes` → `dotnet test` after every single
  task above, not just at checkpoints or at T026** — Constitution Principle VIII treats this as
  blocking per task, and a task is not "done" until all three pass.
