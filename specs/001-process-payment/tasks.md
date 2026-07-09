---

description: "Task list template for feature implementation"
---

# Tasks: Process Payment from Order Placed Event

**Input**: Design documents from `specs/001-process-payment/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md (all present)

**Tests**: Included and REQUIRED — Constitution Principles IV and V mandate unit tests for every
Domain method and Service handler, plus integration and component test coverage, so test tasks
are not optional for this feature.

**⚠️ Build/format/test gate applies to EVERY task, not just the end**: Constitution Principle
VIII requires `dotnet build` → `dotnet format --verify-no-changes` → `dotnet test` to pass after
**each** task is implemented, not only once at the end of the feature. T044 (Phase 6) is the
final full-solution confirmation — it does not replace running the gate after T001, after T002,
and so on. Do not mark any task complete while the gate is red.

**Organization**: Tasks are grouped by user story (from spec.md) to enable independent
implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3)
- Paths follow the layout in [plan.md](./plan.md) — single solution, `src/` + `tests/` at repo
  root, project names `Ecommerce.Payments.{Consumer,Service,Domain,Infrastructure}` and
  `Ecommerce.Payments.{Domain,Service,Integration,Component}.Tests`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Solution/project skeleton and tooling — no feature code yet

- [X] T001 Create `Ecommerce.Payments.sln` and the project skeletons — `src/Ecommerce.Payments.Consumer/`, `src/Ecommerce.Payments.Service/`, `src/Ecommerce.Payments.Domain/`, `src/Ecommerce.Payments.Infrastructure/`, `tests/Ecommerce.Payments.Domain.Tests/`, `tests/Ecommerce.Payments.Service.Tests/`, `tests/Ecommerce.Payments.Integration.Tests/`, `tests/Ecommerce.Payments.Component.Tests/` — and add all of them to the solution, per [plan.md](./plan.md) Project Structure
- [X] T002 [P] Add NuGet package references per [plan.md](./plan.md) Technical Context: `Confluent.Kafka` to Consumer + Infrastructure; `Microsoft.Extensions.Hosting` to Consumer; `Microsoft.EntityFrameworkCore` + `Npgsql.EntityFrameworkCore.PostgreSQL` to Infrastructure; `Polly` to Service; `xunit` + `Moq` to Domain.Tests and Service.Tests; `Testcontainers.PostgreSql` to Integration.Tests; `Testcontainers.PostgreSql` + `Testcontainers.Kafka` to Component.Tests
- [X] T003 [P] Enable `<Nullable>enable</Nullable>` and explicit `<ImplicitUsings>` on every `src/*.csproj` and `tests/*.csproj`; add a Roslyn analyzer rule set that treats `dynamic`/`object` usage on Consumer/Service/Domain/Publisher data as a build error per Constitution Principle III/VIII
- [X] T004 [P] Add a repo-root `.editorconfig` and confirm `dotnet format --verify-no-changes` runs clean on the empty skeleton
- [X] T005 [P] Add a repo-root `docker-compose.yml` with `postgres` and `kafka` services for local/dev, matching [quickstart.md](./quickstart.md) Prerequisites/Setup

**Checkpoint**: Solution builds, no source files yet — ready for foundational work. Run the
build/format/test gate before continuing.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Shared types, persistence schema, and consumer/host plumbing that every user story
builds on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T006 [P] Create `PaymentStatus` enum (`Pending`, `Processed`, `Failed`) in `src/Ecommerce.Payments.Domain/Payments/PaymentStatus.cs` per [data-model.md](./data-model.md)
- [X] T007 [P] Create `InvalidPaymentTransitionException` in `src/Ecommerce.Payments.Domain/Payments/InvalidPaymentTransitionException.cs`
- [X] T008 [P] Create the `PaymentDomainEvent` base type and `PaymentProcessed` domain event in `src/Ecommerce.Payments.Domain/Payments/PaymentDomainEvent.cs` and `src/Ecommerce.Payments.Domain/Payments/PaymentProcessed.cs` per [data-model.md](./data-model.md)
- [X] T009 [P] Create the `Payment` aggregate root shell — `Id`, `OrderId`, `CustomerId`, `Amount`, `Currency`, `Status`, `SourceEventId`, `CreatedAt`, `ProcessedAt`, all with private setters and no public mutators — in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` per [data-model.md](./data-model.md) (the `Create()` factory and `Process()` transition are added in US1, T025/T026)
- [X] T010 [P] Create the `OrderPlacedEvent` DTO, including a `TryParse`/validation path that rejects missing fields, non-UUID ids, non-positive `amount`, or non-3-letter `currency`, in `src/Ecommerce.Payments.Service/IntegrationEvents/OrderPlacedEvent.cs` per [contracts/orders.order-placed.md](./contracts/orders.order-placed.md)
- [X] T011 [P] Create the `PaymentProcessedEvent` DTO in `src/Ecommerce.Payments.Infrastructure/Messaging/IntegrationEvents/PaymentProcessedEvent.cs` per [contracts/payments.payment-processed.md](./contracts/payments.payment-processed.md)
- [X] T012 Create `PaymentsDbContext` with `PaymentEntity` and `PaymentDeadLetterEntity` in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentsDbContext.cs`, `Entities/PaymentEntity.cs`, `Entities/PaymentDeadLetterEntity.cs` per [data-model.md](./data-model.md) (depends on T006)
- [X] T013 Generate the initial EF Core migration creating `payments` (with `UNIQUE (order_id)`) and `payment_dead_letters` in `src/Ecommerce.Payments.Infrastructure/Persistence/Migrations/` (depends on T012)
- [X] T014 [P] Define `IPaymentRepository` and `IPaymentEventPublisher` in `src/Ecommerce.Payments.Service/Payments/IPaymentRepository.cs` and `src/Ecommerce.Payments.Service/Payments/IPaymentEventPublisher.cs`
- [X] T015 Create the Consumer host skeleton — Generic Host builder, configuration, logging, DI container setup — in `src/Ecommerce.Payments.Consumer/Program.cs` (depends on T001)
- [X] T016 Create the `OrderPlacedConsumer` `BackgroundService` skeleton — subscribes to `orders.order-placed`, `EnableAutoCommit = false`, deserializes via `OrderPlacedEvent`, rejects and logs invalid envelopes without forwarding them, wraps the Service call in a catch-all so a bad message never crashes the host — in `src/Ecommerce.Payments.Consumer/Consumers/OrderPlacedConsumer.cs` (depends on T010, T015)
- [X] T017 [P] Add `appsettings.json` with Kafka bootstrap servers, consumer group id, topic names, and the PostgreSQL connection string in `src/Ecommerce.Payments.Consumer/appsettings.json`
- [X] T018 [P] Unit test: `OrderPlacedEvent` parsing/validation accepts a well-formed payload and rejects each malformed/incomplete case (missing field, bad UUID, non-positive amount, bad currency) in `tests/Ecommerce.Payments.Service.Tests/IntegrationEvents/OrderPlacedEventTests.cs` (depends on T010)

**Checkpoint**: Solution has schema, DTOs, host plumbing, and envelope validation in place — user
story implementation can now begin. Run the build/format/test gate before continuing.

---

## Phase 3: User Story 1 - Charge the customer when an order is placed (Priority: P1) 🎯 MVP

**Goal**: A valid `OrderPlaced` message results in a new `Payment` created in `Pending`, moved to
`Processed`, durably saved, and a `PaymentProcessed` event published afterward — in that exact
order.

**Independent Test**: Send one valid `OrderPlaced` message (quickstart.md Scenario 1) and confirm
a `Processed` `Payment` row exists and a matching `PaymentProcessed` message was published.

### Tests for User Story 1

- [X] T019 [P] [US1] Unit test: `Payment.Create()` constructs a new `Payment` in `Pending` with the given `OrderId`/`CustomerId`/`Amount`/`Currency`/`SourceEventId`, and rejects a non-positive `Amount` or malformed `Currency` at construction time, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs` (implemented as `Payment.CreatePending()` — renamed to avoid an unrelated overload-resolution issue with `Create`)
- [X] T020 [P] [US1] Unit test: `Payment.Process()` happy path transitions `Pending → Processed` and raises `PaymentProcessed`, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs`
- [X] T021 [P] [US1] Unit test: `Payment.Process()` on an already-`Processed` payment throws `InvalidPaymentTransitionException` without mutating state, in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs`
- [X] T022 [P] [US1] Unit test: `ProcessPaymentHandler` calls Repository then Publisher in that exact order, and never calls Publisher if Repository throws, using Moq for `IPaymentRepository`/`IPaymentEventPublisher`, in `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`
- [X] T023 [P] [US1] Integration test: a real `OrderPlaced` message results in a correctly persisted `Payment` row (Consumer → Repository) against a containerized PostgreSQL, in `tests/Ecommerce.Payments.Integration.Tests/OrderPlacedToPaymentTests.cs` — verified passing against real Docker/PostgreSQL
- [X] T024 [US1] Component test: full flow with containerized Kafka + PostgreSQL — consume `OrderPlaced` → `Payment` saved → `PaymentProcessed` published to the correct topic with the correct payload, in `tests/Ecommerce.Payments.Component.Tests/ProcessPaymentFlowTests.cs` — verified passing against real Docker/Kafka/PostgreSQL

### Implementation for User Story 1

- [X] T025 [US1] Implement `Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId)` — validates `Amount > 0` and a non-empty 3-letter `Currency`, returns a new aggregate in `Pending` with a freshly generated `Id` and `CreatedAt` set — in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` (depends on T009; makes T019 pass)
- [X] T026 [US1] Implement `Payment.Process()` — transitions `Pending → Processed`, sets `ProcessedAt`, raises `PaymentProcessed`; throws `InvalidPaymentTransitionException` if not currently `Pending` — in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` (depends on T025; makes T020/T021 pass)
- [X] T027 [US1] Implement `PaymentRepository` (EF Core, maps `Payment` ↔ `PaymentEntity`, durable save) in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs` (depends on T012, T014)
- [X] T028 [US1] Implement `PaymentEventPublisher` (Confluent.Kafka producer; serializes the `PaymentProcessed` domain event into `PaymentProcessedEvent` JSON; publishes to `payments.payment-processed`) in `src/Ecommerce.Payments.Infrastructure/Messaging/PaymentEventPublisher.cs` (depends on T011, T014)
- [X] T029 [US1] Implement `ProcessPaymentHandler` orchestrating `Payment.CreatePending()` → `Payment.Process()` → `IPaymentRepository.SaveAsync()` → `IPaymentEventPublisher.PublishAsync()`, strictly in that order, in `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` (depends on T025, T026, T014; makes T022 pass)
- [X] T030 [US1] Wire `OrderPlacedConsumer` to call `ProcessPaymentHandler` on a validated `OrderPlacedEvent` and commit the Kafka offset only after the handler completes successfully, in `src/Ecommerce.Payments.Consumer/Consumers/OrderPlacedConsumer.cs` (depends on T016, T029)
- [X] T031 [US1] Register `PaymentsDbContext`, `IPaymentRepository → PaymentRepository`, `IPaymentEventPublisher → PaymentEventPublisher`, `ProcessPaymentHandler`, and the Kafka consumer/producer clients in DI in `src/Ecommerce.Payments.Consumer/Program.cs` (depends on T027, T028, T029)

**Checkpoint**: User Story 1 is fully functional and independently testable — this is the MVP. Run
the build/format/test gate before continuing.

---

## Phase 4: User Story 2 - Never double-charge on duplicate notifications (Priority: P2)

**Goal**: Redelivery of an `OrderPlaced` message already fully processed is a no-op — no second
`Payment` row, no second `PaymentProcessed` publish.

**Independent Test**: Send the identical `OrderPlaced` message twice (quickstart.md Scenario 2)
and confirm only one `Payment` row and one `PaymentProcessed` publish exist.

### Tests for User Story 2

- [X] T032 [P] [US2] Unit test: `ProcessPaymentHandler` skips Domain/Repository/Publisher entirely when a `Payment` already exists for the incoming `OrderId`, in `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`
- [X] T033 [US2] Component test: redelivering the identical `OrderPlaced` message twice results in exactly one `Payment` row and exactly one `PaymentProcessed` publish, in `tests/Ecommerce.Payments.Component.Tests/DuplicateDeliveryTests.cs` — verified passing against real Docker/Kafka/PostgreSQL

### Implementation for User Story 2

- [X] T034 [US2] Add `IPaymentRepository.ExistsByOrderIdAsync(orderId)` and implement it against the unique `order_id` index, in `src/Ecommerce.Payments.Service/Payments/IPaymentRepository.cs` and `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs` (depends on T013, T027)
- [X] T035 [US2] Update `ProcessPaymentHandler` to check `ExistsByOrderIdAsync` before invoking `Payment.CreatePending()`/`Process()`, short-circuiting as a no-op (no Domain call, no save, no publish) when a `Payment` already exists for the order, in `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` (depends on T029, T034; makes T032 pass)

**Checkpoint**: User Stories 1 AND 2 both work independently. Run the build/format/test gate before
continuing.

---

## Phase 5: User Story 3 - Protect against failures during payment recording (Priority: P3)

**Goal**: A temporary failure while durably saving a `Payment` never results in a false
`PaymentProcessed` publish; it is logged with full context and the original message is retried
rather than lost. (Also closes the related edge case: a publish failure *after* a successful save
is dead-lettered rather than dropped, without rolling back the already-correct `Payment` row.)

**Independent Test**: Simulate a storage disruption during processing (quickstart.md Scenario 3)
and confirm no completion is published, the failure is logged with full context, and the message
is later retried successfully once the disruption clears.

### Tests for User Story 3

- [X] T036 [P] [US3] Unit test: `ProcessPaymentHandler` never calls Publisher when `Repository.SaveAsync` throws after exhausting retries, in `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`
- [X] T037 [US3] Integration test: a transient PostgreSQL failure during save is retried and eventually succeeds without a duplicate `Payment` row or a premature publish, against containerized PostgreSQL, in `tests/Ecommerce.Payments.Integration.Tests/TransientSaveFailureTests.cs` — simulates a real outage via Docker container pause/unpause; verified passing against real Docker/PostgreSQL
- [X] T038 [US3] Component test: when `Publisher.PublishAsync` fails after a successful save, a row is written to `payment_dead_letters` and the Kafka offset is still committed, in `tests/Ecommerce.Payments.Component.Tests/PublishFailureDeadLetterTests.cs` — verified passing against real PostgreSQL with a genuine (unreachable-broker) publish failure

### Implementation for User Story 3

- [X] T039 [US3] Wrap both `IPaymentRepository.ExistsByOrderIdAsync` and `SaveAsync` calls in `ProcessPaymentHandler` with a bounded exponential backoff retry (Polly) for transient failures — both are DB calls equally subject to transient outages, not just the save; on exhaustion, propagate the failure without calling Publisher and log with full context (order id, error detail), in `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` (depends on T029, T035)
- [X] T040 [US3] Ensure `OrderPlacedConsumer` does not commit the Kafka offset when `ProcessPaymentHandler` reports a save failure, so Kafka redelivery drives the next retry, in `src/Ecommerce.Payments.Consumer/Consumers/OrderPlacedConsumer.cs` (depends on T030, T039) — achieved by letting the exhausted-retry exception propagate uncaught out of `HandleMessageAsync`, caught only by `ExecuteAsync`'s outer handler, which skips the commit
- [X] T041 [US3] Implement dead-letter handling: on a publish failure occurring after a successful save, write the failed `PaymentProcessedEvent` payload and failure reason to `payment_dead_letters` and still report overall success to the Consumer so the offset is committed, in `src/Ecommerce.Payments.Infrastructure/Messaging/PaymentEventPublisher.cs` and `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` (depends on T012, T028, T039) — `PaymentEventPublisher` (singleton) uses `IServiceScopeFactory` to reach a scoped `PaymentsDbContext` only on this rare failure path; also added a bounded `MessageTimeoutMs` so a broker outage fails fast instead of hanging

**Checkpoint**: All three user stories are independently functional. Run the build/format/test gate
before continuing.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [X] T042 [P] Add XML doc comments to `Payment`, `PaymentStatus`, `OrderPlacedEvent`, `PaymentProcessedEvent` describing every field, per Constitution Documentation Standards
- [X] T043 [P] Add a `README.md` to each of `src/Ecommerce.Payments.Consumer/`, `src/Ecommerce.Payments.Service/`, `src/Ecommerce.Payments.Domain/`, `src/Ecommerce.Payments.Infrastructure/` describing its responsibility, the topics it consumes/produces, and its position in the Consumer → Service → Domain → Repository → Publisher flow
- [X] T044 Run `dotnet build`, `dotnet format --verify-no-changes`, and `dotnet test` for the full solution as a final confirmation (this is in addition to, not instead of, running the same gate after every preceding task); fix any failure before considering the feature done, per Constitution Principle VIII — 34/34 tests passing solution-wide
- [X] T045 Execute [quickstart.md](./quickstart.md) validation Scenarios 1-4 manually — ran the real `Ecommerce.Payments.Consumer` host against this machine's shared `ecommerce-infra` Kafka + payments PostgreSQL (not the repo's own docker-compose, which port-conflicted with it), producing real messages via `kafka-console-producer` and pausing the real Postgres container for Scenario 3; all 4 scenarios confirmed, test data cleaned up afterward

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational only
- **User Story 2 (Phase 4)**: Depends on Foundational + US1's `ProcessPaymentHandler`/`PaymentRepository` existing (T027, T029) — extends rather than duplicates that file
- **User Story 3 (Phase 5)**: Depends on Foundational + US1 + US2's `ProcessPaymentHandler` state (T029, T035) — extends the same file again
- **Polish (Phase 6)**: Depends on all three user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: No dependencies on other stories — independently testable as-is (the MVP)
- **User Story 2 (P2)**: Builds on US1's handler/repository files but is independently testable via its own scenario (duplicate delivery produces no extra side effects)
- **User Story 3 (P3)**: Builds on US1+US2's handler file but is independently testable via its own scenario (save failure produces no premature publish)

### Within Each User Story

- Tests are written first and MUST fail before the corresponding implementation task
- Domain (`Create()` before `Process()`) before Service before Infrastructure wiring before Consumer wiring
- Story complete and checkpoint validated before moving to the next priority

### Parallel Opportunities

- All Setup tasks marked [P] (T002-T005) run in parallel
- Within Foundational, T006-T011 and T014, T017-T018 (marked [P]) run in parallel; T012-T013 and T015-T016 are sequential chains
- All US1 test tasks marked [P] (T019-T023) run in parallel with each other (not with T024, which needs the full stack)
- T032 (US2 test) can be written in parallel with other work once US1 is checkpointed
- T036 (US3 test) can be written in parallel with other work once US2 is checkpointed

---

## Parallel Example: Foundational Phase

```bash
Task: "Create PaymentStatus enum in src/Ecommerce.Payments.Domain/Payments/PaymentStatus.cs"
Task: "Create InvalidPaymentTransitionException in src/Ecommerce.Payments.Domain/Payments/InvalidPaymentTransitionException.cs"
Task: "Create PaymentDomainEvent base + PaymentProcessed in src/Ecommerce.Payments.Domain/Payments/"
Task: "Create Payment aggregate shell in src/Ecommerce.Payments.Domain/Payments/Payment.cs"
Task: "Create OrderPlacedEvent DTO in src/Ecommerce.Payments.Service/IntegrationEvents/OrderPlacedEvent.cs"
Task: "Create PaymentProcessedEvent DTO in src/Ecommerce.Payments.Infrastructure/Messaging/IntegrationEvents/PaymentProcessedEvent.cs"
```

## Parallel Example: User Story 1 Tests

```bash
Task: "Unit test Payment.Create() in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test Payment.Process() happy path in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test Payment.Process() invalid transition in tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentTests.cs"
Task: "Unit test ProcessPaymentHandler call order in tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs"
Task: "Integration test Consumer->Repository in tests/Ecommerce.Payments.Integration.Tests/OrderPlacedToPaymentTests.cs"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (blocks everything else)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: run quickstart.md Scenario 1 end-to-end
5. This is a deployable MVP — customers get charged on order placement

### Incremental Delivery

1. Setup + Foundational → foundation ready
2. Add User Story 1 → validate via Scenario 1 → MVP
3. Add User Story 2 → validate via Scenario 2 (duplicate protection)
4. Add User Story 3 → validate via Scenario 3 + 4 (failure resilience, malformed messages)
5. Polish → full constitution compliance (build/format/test gate, docs)

---

## Notes

- [P] tasks touch different files with no incomplete dependencies
- [Story] labels map tasks to spec.md's user stories for traceability
- `Payment.Create()` (T025) and `Payment.Process()` (T026) are split into two Domain tasks/tests
  on purpose — FR-002 (create in Pending) and FR-003 (transition to Processed) are distinct
  requirements and each gets its own unit test (T019 vs. T020/T021)
- US2 and US3 intentionally modify `ProcessPaymentHandler.cs` (and, for US3, `OrderPlacedConsumer.cs`/`PaymentEventPublisher.cs`) rather than duplicating it — each still has its own independent test scenario per the spec
- **Run `dotnet build` → `dotnet format --verify-no-changes` → `dotnet test` after every single task above, not just at checkpoints or at T044** — Constitution Principle VIII treats this as blocking per task, and a task is not "done" until all three pass. Checkpoints and T044 are reminders of this, not the only times it applies.

## Post-Implementation Correction (2026-07-09)

After implementation, testing against the real `orders` service (`ecommerce-orders`) revealed
the original `contracts/orders.order-placed.md` assumption was wrong on three fields: there is
no separate `orderId` (the order id is `aggregateId`), no `currency` field at all (the platform
is single-currency), and the amount field is `totalAmount`, not `amount`. `OrderPlacedEvent.cs`,
`ProcessPaymentHandler.cs`, and every test constructing an `OrderPlaced` payload (T018-T024,
T032-T038) were corrected to match the verified real schema; `OrderPlacedEvent.DefaultCurrency`
(`"USD"`) now stands in for the missing currency field. Full gate re-run clean (28/28 tests) and
additionally verified against a live `orders` service instance producing a real event — see
[contracts/orders.order-placed.md](./contracts/orders.order-placed.md)'s "Assumption-check note"
for the detail. This is exactly the risk the original contract doc had flagged as unverified.
