# Implementation Plan: Refund on Order Cancelled

**Branch**: `feature/US-04-refund-on-order-cancelled` | **Date**: 2026-11-12 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/004-refund-on-order-cancelled/spec.md`

## Summary

Payments consumes `orders.order-cancelled`, loads the payment by order id (`aggregateId`), and
lets the Domain decide: a `Processed` payment is refunded (`Processed → Refunded`), anything else
is ignored with an Information log. The refund is saved first with a conditional update (only if
still `Processed`), then `PaymentRefunded` is published to `payments.payment-refunded` (key =
payment id) through the existing publish path, which dead-letters on failure so the US-03 retry
worker delivers it. A new `OrderCancelledConsumer` shares an extracted `KafkaConsumerBase` with
`OrderPlacedConsumer`; the base retries a persistently failing message in place (seek back +
capped exponential backoff) so delivery stays at-least-once. Idempotency: a redelivery is
recognised by its `eventId` (stored on the payment as `RefundSourceEventId`) before the Domain
runs; a different cancellation for an already-refunded order is recognised by the `Refunded`
state. One migration adds `RefundedAt` and `RefundSourceEventId`.

**Reuse inventory** (research R1): there was no consumer base and no processed-events table in
US-01..US-03. Per the user's decisions, the consumer base is extracted now and no inbox table is
added (the `eventId` check uses the column stored on the payment). The dead-letter table and
retry worker are reused unchanged.

## Technical Context

**Language/Version**: C# / .NET 10

**Primary Dependencies**: Confluent.Kafka 2.15.0, EF Core 10.0.4 + Npgsql 10.0.2, Polly 8.7.0,
Microsoft.Extensions.Hosting 10.0.9 (no new production packages; `Service.Tests` gains
`Microsoft.Extensions.TimeProvider.Testing` 10.11.0, already used by other test projects)

**Storage**: PostgreSQL `payments` table (2 new nullable columns); `payment_dead_letters` reused

**Testing**: xUnit, Moq, FakeTimeProvider, Testcontainers (PostgreSQL, Kafka)

**Target Platform**: .NET Worker Service (Linux container / Windows dev)

**Project Type**: Event-driven background worker, no HTTP surface

**Performance Goals**: same per-message cost as OrderPlaced (one read + one conditional update +
one publish)

**Constraints**: save before publish; at most one refund announcement per payment; ignored cases
never throw; no change to OrderPlaced behaviour or existing contracts

**Scale/Scope**: one new topic in, one new topic out; full refunds only

No open NEEDS CLARIFICATION — resolved in [research.md](research.md) (user answered R2/R3).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. Layering | Consumer (`OrderCancelledConsumer`) → Service (`RefundPaymentHandler`) → Domain (`Payment.RequestRefund`) → Repository (`GetByOrderIdAsync`, `SaveRefundAsync`) → Publisher (`PublishRefundedAsync`). Refund rule only in Domain; handler switches on the decision *type*. `Payment.Rehydrate` keeps EF types out of Domain. | ✅ |
| II. Write flow | Load → Domain → conditional save → publish → commit. Save failure: no publish, no commit. Publish failure: dead-letter, commit. | ✅ |
| II. Idempotency (dedupe by `eventId`) | Handler compares the inbound `eventId` with the payment's `RefundSourceEventId` **before** calling Domain → duplicate delivery ignored. The `Refunded` state additionally catches re-publishes with a new `eventId`. No inbox table (user decision). | ✅ |
| III. Type safety | Typed `OrderCancelledEvent`/`PaymentRefundedEvent`; `RefundDecision` result type instead of null/bool for the business decision; strict `Refund()` throws `InvalidPaymentTransitionException`. | ✅ |
| IV/V. Tests | Domain + Service unit tests (happy + edge), Integration (consumer → DB, concurrent update), Component (Kafka flow, redelivery, dead-letter → retry). | ✅ |
| VI. At-least-once commit strategy | `KafkaConsumerBase` seeks back to a failed message's offset and retries it with capped exponential backoff, never committing past it (fixes the gap inherited from `OrderPlacedConsumer`). | ✅ |
| VI. "Retried with backoff before being routed to a dead-letter table/topic" | Retry with backoff: ✅. Routing a permanently failing **inbound** message to a dead-letter store: not implemented — no inbound dead-letter mechanism exists (US-03 scoped it out). Failures retry in place indefinitely, so nothing is lost. | ⚠ open — follow-up decision, see Complexity Tracking |
| VI. Kafka | Consumes `orders.order-cancelled`; publishes `PaymentRefunded` → `payments.payment-refunded`, `aggregateId` = payment id + `orderId`; refund only from Processed; Failed/Refunded/not-found ignored, logged, committed, never throw; dead-letter reuse; startup independent of Kafka (`Task.Yield` in base). | ✅ |
| VII. Branching | `feature/US-04-refund-on-order-cancelled` from up-to-date `main`; `feat(US-04)`/`test(US-04)`/`docs(US-04)`. | ✅ |
| VIII. Quality gate | build → format → test after every task. | ✅ |
| Docs | XML docs on all new public types/fields; contracts documented in event classes; README (events table, hosted services), project READMEs, CLAUDE.md. | ✅ |

**Post-design re-check**: unchanged ✅.

## Design

### Flow (`RefundPaymentHandler.HandleAsync(OrderCancelledEvent)`)

```
payment = retry(GetByOrderIdAsync(aggregateId))
  null            → log Information "no payment for order"            → return (commit)
payment.RefundSourceEventId == eventId
                  → log Information "duplicate delivery"              → return (commit)
decision = payment.RequestRefund(eventId, timeProvider.GetUtcNow())
  Ignored(reason) → log Information (NotCompleted | AlreadyRefunded)  → return (commit)
  Refunded(evt)   → updated = retry(SaveRefundAsync(payment))
                      false → log Information "refunded concurrently" → return (commit)
                    PublishRefundedAsync(evt)   // dead-letters internally on failure
DB failure after retries → log Error, rethrow (consumer does not commit)
```

### Consumer loop (`KafkaConsumerBase`)

```
consume → HandleMessageAsync
  true      → commit; reset backoff
  exception → log Error; Seek(failed offset); wait backoff (1 s, 2 s, 4 s … capped at 60 s,
              cancellable); the same message is consumed again next — nothing later is committed
```

Backoff settings: `Kafka:ConsumerRetryBaseDelayMs` (1000) and `Kafka:ConsumerRetryMaxDelayMs`
(60000).

### Components

| Component | Project / path | Kind |
|---|---|---|
| `PaymentStatus.Refunded`, `Payment.RefundedAt`, `RefundSourceEventId`, `Refund`, `RequestRefund`, `Rehydrate` | Domain `Payments/Payment.cs`, `PaymentStatus.cs` | extend |
| `PaymentRefunded`, `RefundDecision`, `RefundIgnoredReason` | Domain `Payments/` | new |
| `OrderCancelledEvent` (+ `TryParse`) | Service `IntegrationEvents/` | new |
| `RefundPaymentHandler` | Service `Payments/` | new |
| `PersistenceRetry` (shared Polly pipeline) | Service `Payments/` | new (extracted from `ProcessPaymentHandler`) |
| `IPaymentRepository.GetByOrderIdAsync`, `SaveRefundAsync` | Service `Payments/IPaymentRepository.cs` | extend |
| `IPaymentEventPublisher.PublishRefundedAsync` | Service `Payments/IPaymentEventPublisher.cs` | extend |
| `PaymentEntity.RefundedAt`, `RefundSourceEventId`; `PaymentRepository` new methods | Infrastructure `Persistence/` | extend |
| Migration `AddPaymentRefundFields` | Infrastructure `Persistence/Migrations/` | new |
| `PaymentRefundedEvent`; `PaymentEventPublisher.PublishRefundedAsync`; `KafkaOptions.OrderCancelledTopic`, `PaymentRefundedTopic` | Infrastructure `Messaging/` | new / extend |
| `KafkaConsumerBase` (+ seek-back retry with backoff) | Consumer `Consumers/` | new (extracted) |
| `KafkaOptions.ConsumerRetryBaseDelayMs`, `ConsumerRetryMaxDelayMs` | Infrastructure `Messaging/KafkaOptions.cs` | extend |
| `OrderPlacedConsumer` | Consumer `Consumers/` | refactor onto base, same behaviour |
| `OrderCancelledConsumer` | Consumer `Consumers/` | new |
| DI (`RefundPaymentHandler`, `AddHostedService<OrderCancelledConsumer>`), `appsettings.json` topics | Consumer | extend |
| `FakePaymentEventPublisher` (integration tests) | tests | extend with `PublishRefundedAsync` |

## Project Structure

### Documentation (this feature)

```text
specs/004-refund-on-order-cancelled/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── orders.order-cancelled.md
│   └── payments.payment-refunded.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
src/
├── Ecommerce.Payments.Domain/Payments/
│   ├── Payment.cs                 PaymentStatus.cs          (extend)
│   └── PaymentRefunded.cs         RefundDecision.cs         RefundIgnoredReason.cs   (new)
├── Ecommerce.Payments.Service/
│   ├── IntegrationEvents/OrderCancelledEvent.cs                                      (new)
│   └── Payments/RefundPaymentHandler.cs  PersistenceRetry.cs                         (new)
│       IPaymentRepository.cs  IPaymentEventPublisher.cs  ProcessPaymentHandler.cs    (extend)
├── Ecommerce.Payments.Infrastructure/
│   ├── Messaging/IntegrationEvents/PaymentRefundedEvent.cs                           (new)
│   ├── Messaging/PaymentEventPublisher.cs  KafkaOptions.cs                           (extend)
│   └── Persistence/PaymentRepository.cs  Entities/PaymentEntity.cs  Migrations/      (extend)
└── Ecommerce.Payments.Consumer/
    ├── Consumers/KafkaConsumerBase.cs  OrderCancelledConsumer.cs                     (new)
    ├── Consumers/OrderPlacedConsumer.cs                                              (refactor)
    └── Program.cs  appsettings.json  README.md                                       (extend)

tests/
├── Ecommerce.Payments.Domain.Tests/Payments/PaymentRefundTests.cs                    (new)
├── Ecommerce.Payments.Service.Tests/IntegrationEvents/OrderCancelledEventTests.cs    (new)
├── Ecommerce.Payments.Service.Tests/Payments/RefundPaymentHandlerTests.cs            (new)
├── Ecommerce.Payments.Integration.Tests/OrderCancelledToRefundTests.cs               (new)
├── Ecommerce.Payments.Integration.Tests/FakePaymentEventPublisher.cs                 (extend)
└── Ecommerce.Payments.Component.Tests/RefundFlowTests.cs                             (new)
```

**Structure Decision**: existing four `src` projects and five test projects; no new projects.

## Risks

- **Base-class refactor of `OrderPlacedConsumer`**: guarded by its existing integration and
  component tests (constructor and `HandleMessageAsync` signatures unchanged).
- **Behaviour change to `OrderPlacedConsumer`** (deliberate, FR-013): a persistently failing
  message now blocks its partition and is retried in place instead of being skipped. Covered by a
  new component test.
- **Known gaps kept** (research R9): crash between save and publish, cancellation before payment
  exists, no inbound dead-letter for permanently failing messages. Documented; follow-up
  candidates.
- **Mixed timestamps**: `RefundedAt` comes from `TimeProvider`; existing transitions still use
  `DateTimeOffset.UtcNow`. Acceptable; unifying is out of scope.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Constitution VI: permanently failing inbound messages are not routed to a dead-letter table/topic (they retry in place with backoff) | No inbound dead-letter mechanism exists; US-03 explicitly scoped it out and the user asked not to add a second mechanism in this story. Retrying in place loses nothing and keeps at-least-once. | Adding an inbound dead-letter table (or topic) now is a new mechanism needing its own decision; recorded as a follow-up |
| Pre-existing: `OrderPlacedConsumer` deduplicates by order id (`ExistsByOrderIdAsync`), not by `eventId` (Constitution II, added in v1.1.0 after US-01) | Not changed here — FR-013 keeps the order-placed business flow as is | Retrofitting `eventId` dedupe to US-01 belongs in its own change |
