# Implementation Plan: Recognise Redelivered OrderPlaced by Event Identifier

**Branch**: `feature/US-08-order-placed-eventid-dedupe` | **Date**: 2026-11-16 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/008-order-placed-eventid-dedupe/spec.md`

## Summary

`ProcessPaymentHandler` currently asks only whether a payment exists for the order. With this
change it loads the payment (`GetByOrderIdAsync`) inside the per-order transaction and compares
`payment.SourceEventId` with the inbound `eventId` before any Domain call:

- **Same `eventId`**: duplicate delivery.
- **Different `eventId`**: the payment already exists, created by another event.

In both cases nothing is written and one Information line is logged.

`ExistsByOrderIdAsync` becomes unused and is removed. There is no schema, contract or
behaviour change for new orders. The order id stays the guarantee of one payment per order.

## Technical Context

**Language/Version**: C# / .NET 10

**Primary Dependencies**: unchanged; no new packages.

**Storage**: PostgreSQL `payments` table, unchanged. Uses the existing `SourceEventId` and the
unique `order_id` index.

**Testing**: xUnit, Moq and Testcontainers (PostgreSQL, Kafka).

**Target Platform**: .NET Worker Service.

**Project Type**: event-driven background worker with no HTTP surface.

**Performance Goals**: the same single indexed read per message as today.

**Constraints**:
- No behaviour change for orders without a payment.
- No new writes on duplicates.

**Scale/Scope**: one handler, one repository method removed, tests and docs.

No NEEDS CLARIFICATION; see [research.md](research.md).

## Constitution Check

*GATE: must pass before Phase 0 research. Re-checked after Phase 1 design.*

Checked against v1.3.0.

| Principle | Check | Status |
|---|---|---|
| I. Layering | Service compares identifiers. The Repository loads the payment (`GetByOrderIdAsync` → `Rehydrate`). No EF Core types reach the Domain. | ✅ |
| II. Idempotency: dedupe by `eventId` before the Domain runs | `SourceEventId == eventId` is checked before `CreatePending` or `Evaluate`. This closes the pre-existing gap. | ✅ |
| II. One payment per order | The order id is still checked: a different `eventId` with an existing payment is ignored. | ✅ |
| II. Write flow | Duplicates write nothing and record no outbox entry. The create path is unchanged (outbox, US-07). | ✅ |
| III. Type safety | Typed outcome records; no `object` or `dynamic`. | ✅ |
| IV/V. Tests | Unit tests for both new branches; integration tests for the same message and for a new `eventId`; component duplicate test unchanged. | ✅ |
| VI. Kafka | No contract change. Offsets are committed after handling, as today. | ✅ |
| VII. Branching | Branch `feature/US-08-order-placed-eventid-dedupe`, cut from an up-to-date `main`. | ✅ |
| VIII. Quality gate | Build, then format, then test, after every task. | ✅ |
| Docs | README known limitation removed; `CLAUDE.md` idempotency note updated; Service README updated. | ✅ |

**Post-design re-check**: unchanged ✅.

## Design

```
await using tx = unitOfWork.BeginAsync(orderId)            // US-05 lock
payment = repository.GetByOrderIdAsync(orderId)
payment != null && payment.SourceEventId == orderPlaced.EventId → DuplicateDelivery(payment.Id)
payment != null                                              → AlreadyExists(payment.Id, payment.SourceEventId)
marker = markers.GetByOrderIdAsync(orderId); marker != null  → OrderCancelled(marker.SourceEventId)   (unchanged)
create → evaluate → save → outbox → commit                   → Created                               (unchanged)
// after the transaction: one Information line for each non-Created outcome
```

## Project Structure

```text
specs/008-order-placed-eventid-dedupe/  spec.md plan.md research.md data-model.md quickstart.md
                                         checklists/requirements.md tasks.md
src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs                   (change)
src/Ecommerce.Payments.Service/Payments/IPaymentRepository.cs                      (remove ExistsByOrderIdAsync)
src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs             (remove ExistsByOrderIdAsync)
tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs      (change)
tests/Ecommerce.Payments.Integration.Tests/OrderPlacedToPaymentTests.cs            (extend)
README.md  CLAUDE.md  src/Ecommerce.Payments.Service/README.md                     (docs)
```

**Structure decision**: no new projects or files apart from the spec folder.

## Risks

- **Test churn**: about 13 test call sites set up `ExistsByOrderIdAsync`. They move to
  `GetByOrderIdAsync`: `null` means no payment, a rehydrated payment means one exists. With
  Moq's default, `GetByOrderIdAsync` already returns `null`, so most of these setups can simply
  be dropped.

## Complexity Tracking

None. US-08 removes the last pre-existing Complexity Tracking item.
