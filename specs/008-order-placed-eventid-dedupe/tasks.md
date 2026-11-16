---

description: "Task list for US-08 OrderPlaced eventId Dedupe"
---

# Tasks: Recognise Redelivered OrderPlaced by Event Identifier

**Input**: design documents in `specs/008-order-placed-eventid-dedupe/`.

**Prerequisites**: plan.md, spec.md, research.md, data-model.md and quickstart.md, all present.
There is no `contracts/` folder: the external contracts do not change.

**Tests**: included and required. Constitution IV requires a unit test for the happy path and
one for an edge case of every changed handler.

**Organization**: the spec has a single user story, US1. Redeliveries are recognised by event
identifier.

**No Setup or Foundational phase**: there are no new projects, packages or schema.

**Quality gate**: after **every** task, run `dotnet build`, then
`dotnet format --verify-no-changes`, then `dotnet test` (Constitution VIII).

**Commits**: Conventional Commits scoped `US-08`.

## Format: `[ID] [P?] [Story] Description`

---

## Phase 1: User Story 1 — A redelivered OrderPlaced is recognised by its event identifier (P1) 🎯 MVP

**Goal**: within the per-order transaction, compare `payment.SourceEventId` with the inbound
`eventId` before any Domain call.
- **Same id**: a duplicate delivery.
- **Different id**: the payment already exists.

In both cases nothing is written and one Information log line is written.

**Independent Test**: deliver the same `OrderPlaced` twice. There must be one payment, one outbox
entry, and one "duplicate delivery" log line.

### Tests for User Story 1

- [X] T001 [P] [US1] Update `tests/Ecommerce.Payments.Service.Tests/Payments/ProcessPaymentHandlerTests.cs`.
  - Replace every `ExistsByOrderIdAsync` setup or verify with `GetByOrderIdAsync`:
    - `false` setups are deleted, because Moq returns `null` by default.
    - `true` setups return a payment built with `Payment.Rehydrate(...)` and status `Processed`.
  - In `Should_Check_Payment_Before_Marker`, return a rehydrated payment.
  - Add these tests:
    - `Should_Log_Duplicate_Delivery_When_Payment_Was_Created_By_Same_EventId`. The rehydrated
      payment has `sourceEventId` equal to `orderPlaced.EventId`. Expect no `SaveAsync`, no
      outbox call, no marker lookup and no commit. Expect exactly one Information entry
      containing "duplicate delivery", the order id, the event id and the payment id. Capture it
      with `ListLogger`.
    - `Should_Log_Already_Exists_When_Payment_Was_Created_By_Another_EventId`. Use a different
      `sourceEventId`. Expect no writes, and exactly one Information entry containing "already
      exists" and that source event id.
- [X] T002 [P] [US1] Extend `tests/Ecommerce.Payments.Integration.Tests/OrderPlacedToPaymentTests.cs`
  with two tests. Each drives the `OrderPlacedConsumer` against PostgreSQL and uses `OutboxReader`.
  - `Redelivered_OrderPlaced_With_Same_EventId_Leaves_One_Payment_And_One_Outbox_Entry`: the
    same raw message is handled twice, and both calls return `true`.
  - `OrderPlaced_With_New_EventId_For_Existing_Order_Leaves_One_Payment_And_One_Outbox_Entry`.

### Implementation for User Story 1

- [X] T003 [US1] Change `src/Ecommerce.Payments.Service/Payments/ProcessPaymentHandler.cs`.
  - In `CreatePaymentAsync`, replace the `ExistsByOrderIdAsync` check with
    `var existing = await _repository.GetByOrderIdAsync(orderId, ct)`. If it is not null, return:
    - `new Outcome.DuplicateDelivery(existing.Id)` when
      `existing.SourceEventId == orderPlaced.EventId`;
    - `new Outcome.AlreadyExists(existing.Id, existing.SourceEventId)` otherwise.
  - Add both records to `Outcome`.
  - After the delegate, log one Information line per outcome, with the wording from research R3:
    - "Ignoring OrderPlaced {EventId} for order {OrderId}: duplicate delivery; payment
      {PaymentId} was created by it."
    - "Ignoring OrderPlaced {EventId} for order {OrderId}: payment {PaymentId} already exists
      (created by OrderPlaced {SourceEventId})."
  - Update the XML docs to say redeliveries are deduplicated by `eventId` before the Domain runs
    (Constitution II).

  Makes T001 and T002 pass.
- [X] T004 [US1] Remove `ExistsByOrderIdAsync` from
  `src/Ecommerce.Payments.Service/Payments/IPaymentRepository.cs` and from
  `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentRepository.cs`. Update the
  interface's XML doc to name `GetByOrderIdAsync` as the idempotency lookup. Depends on T003.

**Checkpoint**: both Payments consumers deduplicate by `eventId` before the Domain runs.

---

## Phase 2: Polish & Cross-Cutting Concerns

- [X] T005 [P] Update the docs:
  - `README.md`: remove the "Known limitations" bullet about `OrderPlacedConsumer`
    deduplicating by order id. In the idempotency paragraph, say that a redelivery is
    recognised by `eventId`, with the order id as the one-payment-per-order guarantee.
  - `CLAUDE.md`: replace the "(`OrderPlacedConsumer` still dedupes by order id —
    pre-existing.)" note with the `OrderPlaced` rule (`SourceEventId` compared before the
    Domain runs, then the order id).
  - `src/Ecommerce.Payments.Service/README.md`: describe the `ProcessPaymentHandler`
    idempotency.
- [X] T006 Run the full quality gate and `docker build -t ecommerce-payments .`, then mark all
  tasks `[X]`.

---

## Dependencies & Execution Order

- T001 and T002 are tests that can run in parallel. Write them first; they should fail before
  T003.
- T003, then T004.
- T005 can be done at any point after T003. T006 is last.

## Parallel Example

```text
T001 ProcessPaymentHandlerTests     T002 OrderPlacedToPaymentTests     (then T003)
```

## Implementation Strategy

There is one story, so the MVP is the whole feature. Commit as
`feat(US-08): dedupe redelivered OrderPlaced by eventId`, then
`docs(US-08): …`.

A manual check is optional; quickstart.md covers it. The behaviour is fully covered by the unit,
integration and component tests, and nothing external changes.
