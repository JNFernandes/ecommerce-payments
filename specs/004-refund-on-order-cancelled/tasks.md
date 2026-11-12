---

description: "Task list for US-04 Refund on Order Cancelled"
---

# Tasks: Refund on Order Cancelled

**Input**: Design documents from `specs/004-refund-on-order-cancelled/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/ (2 files),
quickstart.md (all present)

**Tests**: Included and REQUIRED — Constitution Principles IV/V require unit tests for every
Domain method and Service handler plus integration and component coverage; quickstart.md maps
each scenario to a test project.

**Organization**: grouped by user story (spec.md): US1 = refund a completed payment (P1),
US2 = ignore cancellations that need no refund (P2).

**No Setup phase**: no new projects or packages — the feature extends the existing solution.

**Quality gate**: after **every** task run `dotnet build` → `dotnet format --verify-no-changes`
→ `dotnet test` (Constitution VIII; Docker must be running).

**Commits**: Conventional Commits scoped `US-04` (`feat(US-04): …`, `test(US-04): …`,
`refactor(US-04): …`, `docs(US-04): …`).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelizable (different files, no dependency on an incomplete task)
- **[Story]**: US1 / US2
- **(new)** marks files that do not exist yet

---

## Phase 1: Foundational (Blocking Prerequisites)

**Purpose**: schema, loading a payment, the inbound contract, shared retry and the consumer base
class — everything both stories need. Existing behaviour (OrderPlaced flow, contracts) stays
identical (FR-013).

**⚠️ CRITICAL**: no user story work can begin until this phase is complete.

- [X] T001 [P] Add `Refunded` to `PaymentStatus` with an XML doc ("full refund recorded; terminal") and fix the stale `Failed` doc ("reserved for a future story…") to describe the real meaning, in `src/Ecommerce.Payments.Domain/Payments/PaymentStatus.cs`
- [X] T002 Add `RefundedAt` (`DateTimeOffset?`, private set) and `RefundSourceEventId` (`Guid?`, private set) with XML docs, and a public static `Rehydrate(Guid id, Guid orderId, Guid customerId, decimal amount, string currency, PaymentStatus status, Guid sourceEventId, DateTimeOffset createdAt, DateTimeOffset? processedAt, string? failureReason, DateTimeOffset? failedAt, DateTimeOffset? refundedAt, Guid? refundSourceEventId)` factory documented as "persistence only — restores a saved aggregate without running business rules", extending the private constructor as needed, in `src/Ecommerce.Payments.Domain/Payments/Payment.cs` (depends on T001)
- [X] T003 [P] Add `RefundedAt` (`DateTimeOffset?`) and `RefundSourceEventId` (`Guid?`) to `src/Ecommerce.Payments.Infrastructure/Persistence/Entities/PaymentEntity.cs`
- [X] T004 Generate migration `AddPaymentRefundFields` (two nullable columns on `payments`, no backfill) with `dotnet ef migrations add AddPaymentRefundFields --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure --output-dir Persistence/Migrations`; run `dotnet format` so it uses file-scoped namespaces like the existing migrations (depends on T003)
- [X] T005 Add `Task<Payment?> GetByOrderIdAsync(Guid orderId, CancellationToken)` (XML doc: null = no payment for the order, a lookup result) to `src/Ecommerce.Payments.Service/Payments/IPaymentRepository.cs` and implement it in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs` with `AsNoTracking()` and `Payment.Rehydrate(...)` mapping every column; also map the two new fields in `SaveAsync` (depends on T002, T003)
- [X] T006 [P] Extract the Polly pipeline from `ProcessPaymentHandler` into `internal static class PersistenceRetry` **(new)** exposing `Pipeline` (same settings: handle `Exception`, 3 retries, 200 ms, exponential) in `src/Ecommerce.Payments.Service/Payments/PersistenceRetry.cs`, and make `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs` use it (behaviour unchanged; existing Service tests must stay green)
- [X] T007 [P] Add `OrderCancelledTopic`, `PaymentRefundedTopic`, `ConsumerRetryBaseDelayMs` (default 1000) and `ConsumerRetryMaxDelayMs` (default 60000) with XML docs to `src/Ecommerce.Payments.Infrastructure/Messaging/KafkaOptions.cs` (update the class XML doc) and `"OrderCancelledTopic": "orders.order-cancelled"`, `"PaymentRefundedTopic": "payments.payment-refunded"`, `"ConsumerRetryBaseDelayMs": 1000`, `"ConsumerRetryMaxDelayMs": 60000` to the `Kafka` section of `src/Ecommerce.Payments.Consumer/appsettings.json`
- [X] T008 [P] Write tests **(new)** in `tests/Ecommerce.Payments.Service.Tests/IntegrationEvents/OrderCancelledEventTests.cs` mirroring `OrderPlacedEventTests.cs`: valid payload (with and without `customerId`/`reason`) parses; missing/empty `eventId`, `aggregateId`, `occurredAt`, `version <= 0` each fail with a field-specific error; malformed JSON and `null` body fail; unknown fields ignored
- [X] T009 Create `OrderCancelledEvent` **(new)** in `src/Ecommerce.Payments.Service/IntegrationEvents/OrderCancelledEvent.cs` mirroring `OrderPlacedEvent`: `[JsonPropertyName]` properties `EventId`, `OccurredAt`, `AggregateId` (doc: the order id), `Version`, `CustomerId` (`Guid?`), `Reason` (`string?`), XML docs referencing `ecommerce-orders/src/domain/events/order-cancelled.event.ts` and contracts/orders.order-cancelled.md, and `static bool TryParse(string rawJson, out OrderCancelledEvent? orderCancelled, out string? error)` validating only the required fields (make T008 pass)
- [X] T010 Extract `KafkaConsumerBase` **(new)** — `public abstract class KafkaConsumerBase : BackgroundService` — in `src/Ecommerce.Payments.Consumer/Consumers/KafkaConsumerBase.cs`: constructor `(IOptions<KafkaOptions>, IServiceScopeFactory, ILogger)` builds the consumer exactly as `OrderPlacedConsumer` does today (manual commit, `AutoOffsetReset.Earliest`, `ConsumerGroupId`); protected `ScopeFactory`, `Logger`, `KafkaOptions`; `protected abstract string Topic`; `internal abstract Task<bool> HandleMessageAsync(string rawMessage, CancellationToken)`; `ExecuteAsync` = `await Task.Yield()`, subscribe to `Topic`, then the current loop (consume, `ConsumeException` log + continue, commit when `HandleMessageAsync` returns true and reset the backoff, exit on cancellation) with one deliberate change (Constitution VI at-least-once, spec FR-010/FR-013): when `HandleMessageAsync` throws, log Error with topic/partition/offset, `Seek(consumeResult.TopicPartitionOffset)` and `await Task.Delay(backoff, stoppingToken)` where backoff starts at `ConsumerRetryBaseDelayMs`, doubles per consecutive failure and is capped at `ConsumerRetryMaxDelayMs` — so the same message is consumed again and nothing after it is committed first; `Dispose` closes and disposes the consumer. XML docs cite Constitution I/II/VI (startup independent of Kafka, at-least-once)
- [X] T011 Refactor `src/Ecommerce.Payments.Consumer/Consumers/OrderPlacedConsumer.cs` to derive from `KafkaConsumerBase` (`Topic` = `KafkaOptions.OrderPlacedTopic`; `HandleMessageAsync` body unchanged; same public constructor signature) and delete the moved loop/Dispose code; all existing Integration/Component tests that drive `OrderPlacedConsumer` must pass unchanged (depends on T010)
- [X] T012 Component test **(new)** `FailingMessage_Is_Retried_In_Place_And_Committed_Only_After_Success` in `tests/Ecommerce.Payments.Component.Tests/KafkaConsumerBaseRetryTests.cs` (Kafka container): a test subclass of `KafkaConsumerBase` (`InternalsVisibleTo` already covers this project) whose `HandleMessageAsync` throws for the first message twice and then succeeds, records every call; produce messages A and B; with `ConsumerRetryBaseDelayMs = 100` assert calls are A, A, A, B (B never handled before A succeeds) and the committed offset (`IConsumer.Committed`, or a fresh consumer in the same group) ends after B (depends on T010)

**Checkpoint**: payments can be loaded as aggregates, the schema has the refund columns, the
OrderCancelled contract parses, and both consumers can share one base. Quality gate green.

---

## Phase 2: User Story 1 — Refund a completed payment when its order is cancelled (Priority: P1) 🎯 MVP

**Goal**: an `OrderCancelled` for a `Processed` payment marks it `Refunded`, saves first, then
publishes `PaymentRefunded` (key = payment id), dead-lettering on publish failure.

**Independent Test**: with a Processed payment, produce `OrderCancelled` for its order → the row is
`Refunded` with `RefundedAt`/`RefundSourceEventId`, and one message with every contract field is on
`payments.payment-refunded`.

### Tests for User Story 1

- [X] T013 [P] [US1] Domain tests **(new)** in `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentRefundTests.cs`: `Refund_Should_Move_Processed_To_Refunded_And_Return_PaymentRefunded` (payment id, order id, amount, currency, `RefundedAt` = given time; `RefundSourceEventId` stored), `Refund_Should_Throw_And_Leave_State_Unchanged_When_Not_Processed` (`[Theory]` Pending, Failed, Refunded), `RequestRefund_Should_Return_Refunded_Decision_For_Processed_Payment`, `Rehydrate_Should_Restore_Every_Field`
- [X] T014 [P] [US1] Handler tests **(new)** in `tests/Ecommerce.Payments.Service.Tests/Payments/RefundPaymentHandlerTests.cs` (Moq `IPaymentRepository`/`IPaymentEventPublisher`, `FakeTimeProvider` — add `Microsoft.Extensions.TimeProvider.Testing` 10.11.0 to `tests/Ecommerce.Payments.Service.Tests/Ecommerce.Payments.Service.Tests.csproj`): `Should_Save_Refund_Before_Publishing_PaymentRefunded` (Moq `MockSequence`), `Should_Publish_PaymentRefunded_With_Payment_Order_Amount_Currency_And_RefundedAt`, `Should_Record_Cancelling_EventId_On_Payment`, `Should_Ignore_Redelivery_With_Same_EventId_Before_Domain` (payment rehydrated as Refunded with `RefundSourceEventId` = inbound `eventId` → no `SaveRefundAsync`, no publish), `Should_Not_Publish_And_Should_Throw_When_Save_Keeps_Failing`, `Should_Retry_Transient_Load_Failure`
- [X] T015 [P] [US1] Integration test **(new)** `HandleMessageAsync_ProcessedPayment_BecomesRefunded` in `tests/Ecommerce.Payments.Integration.Tests/OrderCancelledToRefundTests.cs` (PostgreSQL container; DI registers `TimeProvider.System`, `IPaymentRepository`, `FakePaymentEventPublisher`, `ProcessPaymentHandler`, `RefundPaymentHandler`, `PaymentPolicyOptions`, `KafkaOptions`): seed a Processed payment via `OrderPlacedConsumer.HandleMessageAsync`, run `OrderCancelledConsumer.HandleMessageAsync`, assert committed, row `Refunded`, `RefundedAt` and `RefundSourceEventId` set, and `FakePaymentEventPublisher` recorded one refund for that payment
- [X] T016 [P] [US1] Component tests **(new)** in `tests/Ecommerce.Payments.Component.Tests/RefundFlowTests.cs` (Kafka + PostgreSQL containers; DI mirrors `Program.cs`: `TimeProvider`, both repositories, the single `PaymentEventPublisher` forwarded to both interfaces, both handlers, `IDeadLetterRetryProcessor`, both consumers as hosted services): `FullFlow_OrderPlaced_Then_OrderCancelled_Publishes_PaymentRefunded` — produce `OrderPlaced`, wait for Processed, produce `OrderCancelled`, run both consumers as hosted services, consume `payments.payment-refunded` with `KafkaTestConsumer.SubscribeAndConsume<string>` and assert key = payment id and `eventId`, `aggregateId`, `orderId`, `amount`, `currency`, `refundedAt`, `occurredAt`, `version` = 1; `PublishFailure_DeadLettersRefund_And_RetryWorker_Delivers_It` — refund with the publisher on an unreachable broker (`127.0.0.1:1`) → row `Refunded` + dead letter with `EventType = PaymentRefundedEvent`, `Topic = payments.payment-refunded`, key = payment id; then `DeadLetterRetryProcessor.RunCycleAsync` against the Kafka container → message delivered with the same `eventId`, dead letter `Resolved`

### Implementation for User Story 1

- [X] T017 [P] [US1] Create `PaymentRefunded : PaymentDomainEvent` **(new)** (`Amount`, `Currency`, `RefundedAt`) in `src/Ecommerce.Payments.Domain/Payments/PaymentRefunded.cs`, and `RefundIgnoredReason` **(new)** (`NotCompleted`, `AlreadyRefunded`) in `src/Ecommerce.Payments.Domain/Payments/RefundIgnoredReason.cs`, with XML docs
- [X] T018 [P] [US1] Create `RefundDecision` **(new)** abstract record with nested sealed `Refunded(PaymentRefunded Event)` and `Ignored(RefundIgnoredReason Reason)` and XML docs, in `src/Ecommerce.Payments.Domain/Payments/RefundDecision.cs` (depends on T017)
- [X] T019 [US1] Add `Refund(Guid sourceEventId, DateTimeOffset refundedAt)` (Processed → Refunded, sets `RefundedAt`/`RefundSourceEventId`, returns `PaymentRefunded`; otherwise throws `InvalidPaymentTransitionException`, state unchanged) and `RequestRefund(Guid sourceEventId, DateTimeOffset refundedAt)` (Processed → `Refunded(Refund(...))`; Refunded → `Ignored(AlreadyRefunded)`; Failed/Pending → `Ignored(NotCompleted)`; never throws) to `src/Ecommerce.Payments.Domain/Payments/Payment.cs`; update the class XML doc (depends on T002, T018)
- [X] T020 [US1] Add `Task<bool> SaveRefundAsync(Payment payment, CancellationToken)` (XML doc: conditional on the stored status still being Processed; false = refunded concurrently) to `src/Ecommerce.Payments.Service/Payments/IPaymentRepository.cs` and implement it in `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs` with `Payments.Where(p => p.Id == payment.Id && p.Status == PaymentStatus.Processed).ExecuteUpdateAsync(s => s.SetProperty(Status, Refunded).SetProperty(RefundedAt, …).SetProperty(RefundSourceEventId, …))` returning `rows == 1`
- [X] T021 [P] [US1] Create `PaymentRefundedEvent` **(new)** sealed record mirroring `PaymentProcessedEvent` (`eventId`, `occurredAt`, `aggregateId`, `version`, `orderId`, `amount`, `currency`, `refundedAt`, all `[JsonPropertyName]`, XML docs per field referencing contracts/payments.payment-refunded.md) in `src/Ecommerce.Payments.Infrastructure/Messaging/IntegrationEvents/PaymentRefundedEvent.cs`
- [X] T022 [US1] Add `Task PublishRefundedAsync(PaymentRefunded paymentRefunded, CancellationToken)` to `src/Ecommerce.Payments.Service/Payments/IPaymentEventPublisher.cs`; implement it in `src/Ecommerce.Payments.Infrastructure/Messaging/PaymentEventPublisher.cs` (new `eventId`, `OccurredAt = RefundedAt`, `AggregateId = PaymentId`, `Version = 1`, then the existing `PublishOrDeadLetterAsync` with `KafkaOptions.PaymentRefundedTopic` and `nameof(PaymentRefundedEvent)`); add a recording `PublishRefundedAsync` to `tests/Ecommerce.Payments.Integration.Tests/FakePaymentEventPublisher.cs` (depends on T017, T021)
- [X] T023 [US1] Create `RefundPaymentHandler` **(new)** in `src/Ecommerce.Payments.Service/Payments/RefundPaymentHandler.cs` (ctor: `IPaymentRepository`, `IPaymentEventPublisher`, `TimeProvider`, `ILogger`): `HandleAsync(OrderCancelledEvent, CancellationToken)` per plan.md "Flow" — load via `PersistenceRetry` → if `payment.RefundSourceEventId == orderCancelled.EventId` return before Domain (`eventId` dedupe, Constitution II) → `RequestRefund(eventId, now)` → on `Refunded` call `SaveRefundAsync` via `PersistenceRetry` then `PublishRefundedAsync`; DB failure after retries → Error log with order id and event id, rethrow. For now `null` payment, `Ignored` and `SaveRefundAsync == false` simply return (logging added in US2, T030). XML docs cite Constitution II/VI (depends on T005, T006, T019, T020, T022)
- [X] T024 [US1] Create `OrderCancelledConsumer : KafkaConsumerBase` **(new)** in `src/Ecommerce.Payments.Consumer/Consumers/OrderCancelledConsumer.cs`: `Topic` = `KafkaOptions.OrderCancelledTopic`; `HandleMessageAsync` = `OrderCancelledEvent.TryParse` (malformed → Warning log, return true) → scoped `RefundPaymentHandler.HandleAsync` → return true (exceptions propagate to the base, no commit); XML docs (depends on T009, T010, T023)
- [X] T025 [US1] Register `AddScoped<RefundPaymentHandler>()` and `AddHostedService<OrderCancelledConsumer>()` in `src/Ecommerce.Payments.Consumer/Program.cs` (depends on T024)

**Checkpoint**: MVP — cancelled orders with a completed payment are refunded and announced.
Quality gate green.

---

## Phase 3: User Story 2 — Ignore cancellations that need no refund (Priority: P2)

**Goal**: no payment / Failed / Pending / already Refunded / refunded concurrently → one
Information log naming the order, the event and the reason; no change, no publish, no error,
offset committed. Malformed messages rejected and committed.

**Independent Test**: for each case, deliver `OrderCancelled` → `HandleMessageAsync` returns true,
the row is unchanged, nothing is published, and one Information log line carries the reason.

### Tests for User Story 2

- [X] T026 [P] [US2] Extend `tests/Ecommerce.Payments.Domain.Tests/Payments/PaymentRefundTests.cs`: `RequestRefund_Should_Ignore_Failed_Payment_As_NotCompleted`, `RequestRefund_Should_Ignore_Pending_Payment_As_NotCompleted`, `RequestRefund_Should_Ignore_Refunded_Payment_As_AlreadyRefunded`, each asserting state, `RefundedAt` and `RefundSourceEventId` unchanged
- [X] T027 [P] [US2] Extend `tests/Ecommerce.Payments.Service.Tests/Payments/RefundPaymentHandlerTests.cs` (capturing `ILogger`): `Should_Ignore_And_Log_Information_When_No_Payment_Exists`, `Should_Ignore_And_Log_Information_When_Payment_Failed`, `Should_Ignore_And_Log_Information_When_Payment_Pending`, `Should_Ignore_And_Log_Information_When_Already_Refunded`, `Should_Log_Information_For_Duplicate_EventId`, `Should_Not_Publish_When_Refund_Was_Saved_Concurrently` (`SaveRefundAsync` returns false) — each: no `SaveRefundAsync` (except the last), no publish, no exception, exactly one Information entry containing the order id and event id, no Warning/Error
- [X] T028 [P] [US2] Extend `tests/Ecommerce.Payments.Integration.Tests/OrderCancelledToRefundTests.cs`: `HandleMessageAsync_NoPayment_IsCommittedAndChangesNothing`, `HandleMessageAsync_FailedPayment_StaysFailed` (seed with an amount over `MaxAmountThreshold`), `HandleMessageAsync_SecondCancellation_DoesNotRefundTwice` (two cancellations with different `eventId`s → `RefundSourceEventId` = the first, one recorded publish), `HandleMessageAsync_MalformedMessage_IsCommitted`, and `SaveRefundAsync_Concurrent_OnlyOneSucceeds` (two `PaymentRepository` instances on separate contexts, `Task.WhenAll`, exactly one `true`)
- [X] T029 [P] [US2] Component test `Redelivered_OrderCancelled_Produces_Exactly_One_PaymentRefunded` in `tests/Ecommerce.Payments.Component.Tests/RefundFlowTests.cs`: produce the same `OrderCancelled` twice, wait, consume `payments.payment-refunded` until a short idle timeout and assert exactly one message for the payment

### Implementation for User Story 2

- [X] T030 [US2] In `src/Ecommerce.Payments.Service/Payments/RefundPaymentHandler.cs` replace the silent returns with one `LogInformation` each, including order id, cancellation `eventId` and (when present) the cancellation `reason`: "no payment exists for order", "duplicate delivery of cancellation {EventId}; payment {PaymentId} already refunded by it", "payment {PaymentId} is {Status} and was never completed; nothing to refund" (`NotCompleted`), "payment {PaymentId} was already refunded" (`AlreadyRefunded` and `SaveRefundAsync == false`)
- [X] T031 [US2] Make T026–T029 pass; fix anything they reveal in `src/Ecommerce.Payments.Service/Payments/RefundPaymentHandler.cs` or `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs` (expected: none beyond T030)

**Checkpoint**: saga's "payment failed → order cancelled" path and redeliveries are quiet no-ops.
Quality gate green.

---

## Phase 4: Polish & Cross-Cutting Concerns

- [X] T032 [P] Update root `README.md`: event flow and events table (`OrderCancelled`/`PaymentRefunded` → Implemented), background services (`OrderCancelledConsumer` → Implemented (US-04)), configuration reference (`Kafka:OrderCancelledTopic`, `Kafka:PaymentRefundedTopic`), and a short "Known limitations" note (research R9)
- [X] T033 [P] Update `src/Ecommerce.Payments.Consumer/README.md` (hosted services, topics, `KafkaConsumerBase`), `src/Ecommerce.Payments.Service/README.md` (`RefundPaymentHandler`, `OrderCancelledEvent`), `src/Ecommerce.Payments.Domain/README.md` (Refunded state, `Refund`/`RequestRefund`), `src/Ecommerce.Payments.Infrastructure/README.md` (published `payments.payment-refunded`, new repository methods)
- [X] T034 [P] Update `CLAUDE.md` event contracts table (OrderCancelled consumed, PaymentRefunded published — no longer "Planned"; field lists), idempotency section (redeliveries deduplicated by `eventId` via `RefundSourceEventId` before Domain, then the Refunded state), and new consumers derive from `KafkaConsumerBase`; mention `payments.payment-refunded` in `docs/dead-letter-retry.md` where topics are listed
- [X] T035 Verify XML doc comments on every new/changed public type and member in Domain `Payments/`, Service `Payments/` and `IntegrationEvents/`, Infrastructure `Messaging/` and `Persistence/`, and Consumer `Consumers/`
- [ ] T036 **(skipped — see note)** Run the manual check in `specs/004-refund-on-order-cancelled/quickstart.md` ("Manual check against ecommerce-infra") if the shared stack can be used without disrupting other services; otherwise record why it was skipped
- [X] T037 Final quality gate: `dotnet build`, `dotnet format --verify-no-changes`, `dotnet test` all green; `docker build -t ecommerce-payments .` succeeds

---

## Dependencies & Execution Order

- **Foundational (T001–T012)** → **US1 (T013–T025)** → **US2 (T026–T031)** → **Polish (T032–T037)**.
- US2 builds on US1's handler and domain decision; it adds logging and proves the ignore paths.

### Within Foundational

T001 → T002 → T005; T003 → T004; T003 + T002 → T005; T008 → T009; T010 → T011, T012; T007 → T010 (retry settings). T006 and T007 are
independent.

### Within US1

Tests T013–T016 first (they fail), then T017 → T018 → T019; T020 (after T005); T021 → T022;
T019 + T020 + T022 → T023 → T024 → T025.

## Parallel Examples

```text
# Foundational
T001 PaymentStatus | T003 PaymentEntity | T006 PersistenceRetry | T007 KafkaOptions/appsettings | T008 parser tests

# US1 tests
T013 domain | T014 handler | T015 integration | T016 component

# US1 implementation
T017 PaymentRefunded + reason enum | T021 PaymentRefundedEvent DTO

# US2 tests
T026 domain | T027 handler | T028 integration | T029 component

# Polish
T032 root README | T033 project READMEs | T034 CLAUDE.md + dead-letter doc
```

## Implementation Strategy

1. **Foundational** — includes the consumer-base refactor; existing tests prove OrderPlaced is
   unaffected before any refund code exists.
2. **MVP = US1** — refunds and announcements work; non-refundable cases are already safe (the
   Domain returns `Ignored`, the handler returns and the offset is committed) but not yet logged.
3. **US2** — explicit log lines and full proof of the ignore/redelivery/concurrency paths.
4. **Polish** — docs, manual check, final gate.

Commit after each phase (or logical group) with the quality gate green.

## Implementation notes

- **T036 skipped**: a local `Ecommerce.Payments.Consumer` (PID 38152) was running against the
  shared `ecommerce-infra` stack. Running a second instance in the same consumer group, or
  producing test orders and cancellations there, would interfere with it and with the other
  services. The same scenarios run in the Component tests against real Kafka and PostgreSQL
  containers (full flow, redelivery, dead-letter → retry). Run quickstart.md "Manual check"
  to close this task.
- **Quality gate configuration**: that running process locks `src/Ecommerce.Payments.Consumer/bin/Debug`,
  so the gate ran with `-c Release`.
- **Behaviour change (from `/speckit-analyze`)**: `KafkaConsumerBase` retries a failing message in
  place instead of moving past it; this applies to `OrderPlacedConsumer` too
  (`KafkaConsumerBaseRetryTests`).
- **Open follow-ups**: inbound dead-letter for permanently failing messages; transactional
  outbox (crash between save and publish); `eventId` dedupe for `OrderPlacedConsumer`;
  cancellation that arrives before its payment exists.
