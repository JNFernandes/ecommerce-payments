# Implementation Plan: Do Not Charge an Already-Cancelled Order

**Branch**: `feature/US-05-cancellation-marker` | **Date**: 2026-11-15 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/005-cancellation-marker/spec.md`

## Summary

US-04 ignores an `OrderCancelled` that arrives before the order's payment exists. US-05 records
a **cancellation marker** in that case instead, in a new `order_cancellation_markers` table
whose primary key is `OrderId`. An `OrderPlaced` for a marked order then creates no payment and
publishes nothing.

Both handlers run their read-decide-write step inside one transaction. The first statement of
that transaction takes a **per-order PostgreSQL advisory lock**
(`pg_advisory_xact_lock(orderKey)`). The second event for the same order waits for the first to
commit and then sees its marker or payment. Whichever commits first wins, and the race in
FR-006 is closed. The design is the same as Inventory's (`ecommerce-inventory` ADR 0005) and is
recorded in a new Payments ADR, 0004.

There are no new Kafka events or contract changes. One migration is added.

## Technical Context

**Language/Version**: C# / .NET 10

**Primary Dependencies**: Confluent.Kafka 2.15.0, EF Core 10.0.4 + Npgsql 10.0.2, Polly 8.7.0.
No new packages.

**Storage**: PostgreSQL. New table `order_cancellation_markers` (PK `OrderId`). `payments` and
`payment_dead_letters` are unchanged.

**Testing**: xUnit, Moq, FakeTimeProvider, Testcontainers (PostgreSQL, Kafka)

**Target Platform**: .NET Worker Service (Linux container / Windows dev)

**Project Type**: Event-driven background worker, no HTTP surface

**Performance Goals**: One extra short transaction statement per message (the advisory lock)
and one extra indexed read (the marker). Different orders still run in parallel.

**Constraints**:
- Save before publish.
- Never both a marker and a payment for one order.
- Offsets are committed only after the commit.
- Lock waits are bounded by `Persistence:OrderLockTimeoutMs` (default 5000).

**Scale/Scope**: Two existing handlers change; one new table; no contract change.

No open NEEDS CLARIFICATION. Every decision is in [research.md](research.md).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Checked against v1.2.0.

| Principle | Check | Status |
|---|---|---|
| I. Layering | Consumers unchanged → Service handlers → Domain (`Payment`, new `CancellationMarker.Record`) → Repository (`IPaymentRepository`, new `ICancellationMarkerRepository`) → Publisher. The transaction boundary is coordinated by the Service through the `IOrderUnitOfWork` port. EF Core and SQL stay in Infrastructure. | ✅ |
| I. Service has no business rules | The handlers only branch on presence (payment, marker, same `eventId`), as the existing idempotency checks do. The refund and evaluation rules stay in Domain. | ✅ |
| II. Write flow | Lock → read → Domain → save → commit → publish → offset commit. A failed save publishes nothing and commits nothing. A failed publish dead-letters through the existing path. The marker write publishes nothing. | ✅ |
| II. Idempotency | A redelivered cancellation is matched by `eventId` against `RefundSourceEventId` or `CancellationMarker.SourceEventId` before Domain runs. A redelivered placement for a marked order is again a no-op. No payment for a marked order, and no marker for an order with a payment, is guaranteed by the lock. | ✅ |
| III. Type safety | Typed marker entity and domain type. `ArgumentException` on invalid ids. No `object` or `dynamic`. | ✅ |
| IV/V. Tests | Domain: marker factory (valid and invalid). Service: both handlers' new branches with mocks. Integration: marker persistence, migration, 50-round race, forced orders, a no-lock variant showing the race. Component: cancel-before-place on real Kafka. | ✅ |
| VI. Refund rules / Cancellation markers | No payment → marker, logged, committed, nothing published. Marked `OrderPlaced` → no payment and no event. One marker per order enforced by the PK. Mutual exclusion through the advisory lock. Losing the race is logged at Information. Markers are never expired. | ✅ |
| VI. Kafka | No new topics, consumers or events. At-least-once is unchanged (`KafkaConsumerBase`). Startup does not depend on Kafka (unchanged). | ✅ |
| VI. Inbound dead-letter | Unchanged from US-04: a message that keeps failing retries in place, still with no inbound dead-letter. | ⚠ pre-existing; resolved by follow-up US-06 (see Complexity Tracking) |
| VII. Branching | `feature/US-05-cancellation-marker`, branched from an up-to-date `main`. Commits use `feat(US-05)`, `test(US-05)` and `docs(US-05)`. | ✅ |
| VIII. Quality gate | Build, format, then test after every task. | ✅ |
| Docs | ADR 0004. XML docs on all new public types. Root README (known limitations, behaviour, setting), project READMEs, CLAUDE.md. | ✅ |

**Post-design re-check**: unchanged ✅.

## Design

### Unit of work (`IOrderUnitOfWork` → `OrderUnitOfWork`)

```
BeginAsync(orderId):
  tx = dbContext.Database.BeginTransactionAsync()
  SET LOCAL lock_timeout = '{OrderLockTimeoutMs}ms'
  SELECT pg_advisory_xact_lock({OrderLockKey.For(orderId)})   -- waits for any other tx on this order
  return OrderTransaction(tx)
CommitAsync → tx.CommitAsync         (lock released)
DisposeAsync without commit → rollback + ChangeTracker.Clear()   (lock released)
```

### `ProcessPaymentHandler` (OrderPlaced)

```
outcome = retry(async () => {
  await using tx = unitOfWork.BeginAsync(orderId)
  if payments.ExistsByOrderId(orderId)        → return Skip            (existing idempotency)
  if markers.GetByOrderId(orderId) is not null → return SkipCancelled
  payment = Payment.CreatePending(...); evt = payment.Evaluate(threshold)
  payments.Save(payment); tx.Commit()
  return Publish(evt)
})
SkipCancelled → log Information "order {OrderId} already cancelled (marker {SourceEventId}); not charging"
Publish(evt)  → PublishAsync / PublishFailedAsync   (after commit; dead-letters on failure)
DB failure after retries → log Error, rethrow (no commit)
```

### `RefundPaymentHandler` (OrderCancelled)

```
outcome = retry(async () => {
  await using tx = unitOfWork.BeginAsync(orderId)
  payment = payments.GetByOrderId(orderId)
  if payment is null:
     marker = markers.GetByOrderId(orderId)
     marker is null              → markers.Add(CancellationMarker.Record(...)); tx.Commit(); return MarkerRecorded
     marker.SourceEventId == eventId → return DuplicateMarker
     otherwise                   → return AlreadyMarked
  (US-04 logic unchanged: eventId dedupe → RequestRefund → Ignored | SaveRefund → tx.Commit())
})
log per outcome (Information); Refunded → PublishRefundedAsync after commit
```

A transient failure, a lock timeout or an unexpected PK violation rolls back the attempt and
re-runs the whole unit. After the retries run out, the handler logs an Error and rethrows, and
the consumer retries the message in place.

### Components

| Component | Project / path | Kind |
|---|---|---|
| `CancellationMarker` | Domain `Orders/` | new |
| `IOrderUnitOfWork`, `IOrderTransaction`, `ICancellationMarkerRepository` | Service `Orders/` | new |
| `ProcessPaymentHandler`, `RefundPaymentHandler` | Service `Payments/` | change (unit of work, marker branches) |
| `CancellationMarkerEntity`, `CancellationMarkerRepository`, `OrderUnitOfWork`, `OrderLockKey`, `PersistenceOptions` | Infrastructure `Persistence/` | new |
| `PaymentsDbContext` (+ `CancellationMarkers`) | Infrastructure `Persistence/` | extend |
| Migration `AddOrderCancellationMarkers` | Infrastructure `Persistence/Migrations/` | new |
| DI: `IOrderUnitOfWork`, `ICancellationMarkerRepository`, `PersistenceOptions` (validated); `appsettings.json` `Persistence:OrderLockTimeoutMs` | Consumer `Program.cs` | extend |
| ADR 0004, README, project READMEs, CLAUDE.md | docs | new / extend |

## Project Structure

### Documentation (this feature)

```text
specs/005-cancellation-marker/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/order-events-handling.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
src/
├── Ecommerce.Payments.Domain/Orders/CancellationMarker.cs                              (new)
├── Ecommerce.Payments.Service/
│   ├── Orders/IOrderUnitOfWork.cs  IOrderTransaction.cs  ICancellationMarkerRepository.cs (new)
│   └── Payments/ProcessPaymentHandler.cs  RefundPaymentHandler.cs                      (change)
├── Ecommerce.Payments.Infrastructure/Persistence/
│   ├── Entities/CancellationMarkerEntity.cs                                            (new)
│   ├── CancellationMarkerRepository.cs  OrderUnitOfWork.cs  OrderLockKey.cs
│   │   PersistenceOptions.cs                                                           (new)
│   ├── PaymentsDbContext.cs                                                            (extend)
│   └── Migrations/<timestamp>_AddOrderCancellationMarkers.cs                           (new)
└── Ecommerce.Payments.Consumer/Program.cs  appsettings.json  README.md                 (extend)

tests/
├── Ecommerce.Payments.Domain.Tests/Orders/CancellationMarkerTests.cs                   (new)
├── Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs             (extend)
├── Ecommerce.Payments.Service.Tests/Payments/RefundPaymentHandlerTests.cs              (extend)
├── Ecommerce.Payments.Integration.Tests/CancellationBeforePaymentTests.cs              (new)
├── Ecommerce.Payments.Integration.Tests/PlaceCancelRaceTests.cs                        (new)
├── Ecommerce.Payments.Integration.Tests/CancellationMarkerMigrationTests.cs            (new)
└── Ecommerce.Payments.Component.Tests/CancelBeforePlaceFlowTests.cs                    (new)

docs/adr/0004-per-order-serialisation.md                                                (new)
```

**Structure Decision**: Keep the existing four `src` projects and five test projects; add no
new projects. Existing tests that construct the handlers directly gain the two new
dependencies: a pass-through unit-of-work fake in unit tests, and the real one in integration
tests.

## Risks

- **Handler constructor changes** break existing tests that build `ProcessPaymentHandler` or
  `RefundPaymentHandler` directly. Update them in the same task. The behaviour is guarded by
  the unchanged assertions.
- **Retry around a transaction**: a `DbContext` left with tracked `Added` entities after a
  failure would break the retry. Mitigated by clearing the `ChangeTracker` on rollback; covered
  by the existing `TransientSaveFailureTests`, adapted.
- **Lock contention** only between events for the same order, at most two per order. A lock
  timeout is treated as transient.
- **Advisory-key collision** between two different orders only serialises them briefly; it
  never causes a wrong result.
- **Rows created before this feature**: markers only start being written after deployment.
  Earlier ignored early cancellations remain a known historical gap and are not backfilled.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Pre-existing (US-04): permanently failing inbound messages retry in place and are not routed to an inbound dead-letter store (Constitution VI) | Unchanged by this story | **Decided 2026-11-15**: resolved by a separate follow-up story, **US-06 inbound dead-letter**. After N consumer retries, store the raw message in an inbound dead-letter table and commit, as Inventory does with `consumed_dead_letters`. The constitution stays as is. |
| Pre-existing: `OrderPlacedConsumer` dedupes by order id, not `eventId` | The marker branch is also `eventId`-agnostic by design (any placement of a marked order is skipped) | Retrofitting `eventId` dedupe to US-01 is a separate change |
