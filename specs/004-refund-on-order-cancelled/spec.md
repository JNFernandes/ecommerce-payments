# Feature Specification: Refund on Order Cancelled

**Feature Branch**: `feature/US-04-refund-on-order-cancelled`

**Created**: 2026-11-12

**Status**: Draft

**Input**: User description: "US-04: Refund a payment when its order is cancelled. Payments consumes `orders.order-cancelled` (OrderCancelledEvent, aggregateId = order id) and refunds the payment that belongs to that order. After a successful refund it publishes `payments.payment-refunded`." (full rules as provided with `/speckit-specify`)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Refund a completed payment when its order is cancelled (Priority: P1)

When an order whose payment was completed is cancelled, the payments system refunds that payment,
records it as refunded, and announces the refund so the rest of the platform (orders, customer
notifications) can react. The customer is never left charged for an order they will not receive.

**Why this priority**: This is the compensation step of the order/payment saga. Without it a
cancelled order keeps the customer's money.

**Independent Test**: With a completed payment for an order, deliver an order-cancelled
notification for that order and confirm the payment is recorded as refunded and a
payment-refunded announcement is published with the payment, order, amount, currency and refund
time.

**Acceptance Scenarios**:

1. **Given** a completed (Processed) payment for an order, **When** an order-cancelled
   notification for that order arrives, **Then** the payment is marked Refunded with the time of
   the refund.
2. **Given** the refund has been recorded, **When** the record is durably saved, **Then** a
   payment-refunded announcement is published containing: a new event identifier, the payment
   identifier (as the event's aggregate identifier and as its message key), the order
   identifier, the full amount and currency of the payment, the refund time, the event time and
   the schema version.
3. **Given** the refund cannot be durably saved because of a temporary database problem,
   **When** this is detected, **Then** nothing is announced, the problem is retried with
   increasing waits, and if it still fails the notification is handled exactly like a failed
   order-placed notification (logged with full context, not acknowledged).
4. **Given** the refund was saved but announcing it fails, **When** this is detected, **Then**
   the announcement is parked with the existing dead-letter mechanism (US-03) and delivered
   later by the existing retry worker; the notification is still acknowledged and the refund is
   not undone.

---

### User Story 2 - Ignore cancellations that need no refund (Priority: P2)

Some order-cancelled notifications must not cause a refund: the order never had a payment, the
payment had already failed (Orders cancels an order after its payment fails, and that
cancellation comes back to Payments), or the payment was already refunded. The payments system
recognises these cases, writes one informational log line explaining why, acknowledges the
notification, and changes nothing.

**Why this priority**: Without this, the normal "payment failed → order cancelled" saga path
would raise errors or retry forever, and redeliveries could refund twice. It builds on the
refund path from User Story 1.

**Independent Test**: Deliver an order-cancelled notification for (a) an order with no payment,
(b) an order whose payment failed, (c) an order whose payment is already refunded; for each,
confirm nothing changes, nothing is announced, no error is raised, the notification is
acknowledged, and an informational log line names the order and the reason.

**Acceptance Scenarios**:

1. **Given** no payment exists for the cancelled order, **When** the notification arrives,
   **Then** it is acknowledged, nothing is announced, and an informational log line says no
   payment exists for that order.
2. **Given** the order's payment is Failed, **When** the notification arrives, **Then** the
   payment stays Failed, nothing is announced, the notification is acknowledged, and an
   informational log line says the payment was never completed.
3. **Given** the order's payment is already Refunded, **When** the same or another
   order-cancelled notification for that order arrives, **Then** the payment is unchanged, no
   second refund announcement is published, the notification is acknowledged, and an
   informational log line says it was already refunded.
4. **Given** an order-cancelled notification is malformed or missing required fields, **When**
   it arrives, **Then** it is rejected before any business logic, logged as a warning, and
   acknowledged so it is not redelivered forever — the same treatment as malformed
   order-placed notifications.

---

### Edge Cases

- **Redelivery after a successful refund but before acknowledgement**: the payment is already
  Refunded, so the redelivery is ignored (User Story 2 #3); no second refund or announcement.
- **Two different cancellation notifications for the same order** (the orders service creates a
  new event identifier on every publish attempt): the second finds the payment Refunded and is
  ignored.
- **Cancellation arrives before the payment exists** (the two topics are not ordered relative to
  each other): the cancellation is ignored as "no payment"; if the order-placed notification is
  processed afterwards, that payment is not refunded by this feature. Accepted as a known
  limitation for this story (see Assumptions).
- **Persistent processing failure** (e.g. database down longer than the in-handler retries):
  the notification is not acknowledged and the consumer retries the **same** notification with
  increasing waits; later notifications on that partition wait behind it, so nothing is skipped.
  This also applies to the existing order-placed consumer, which shares the same consumer loop.
- **Process crash after the refund is saved but before it is announced or dead-lettered**: the
  redelivery finds the payment Refunded and announces nothing, so that announcement is lost. The
  existing order-placed flow has the same gap (no transactional outbox); accepted for this story.
- **Payment still Pending**: not reachable in practice (a payment is evaluated and saved in one
  step), but if found it is ignored with a log line like the other non-refundable states.
- **Concurrent processing of two cancellations for the same order**: at most one refund is
  recorded and announced.
- **Optional cancellation reason** in the notification is accepted and logged; it does not
  affect the outcome.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST consume order-cancelled notifications from the orders service and
  identify the order by the notification's aggregate identifier (the order id).
- **FR-002**: The system MUST find the payment for the order by order id.
- **FR-003**: Only a payment in the Processed (completed) state MAY be refunded; refunding moves
  it to Refunded and records the refund time. The full amount is refunded.
- **FR-004**: The system MUST ignore — with one informational log line naming the order, the
  event and the reason, no state change, no announcement and no error — notifications whose
  order has no payment, a Failed payment, an already Refunded payment, or (defensively) a
  Pending payment.
- **FR-005**: Processing MUST be idempotent: redelivering a notification, or receiving another
  notification for an already-refunded order, MUST NOT refund twice or announce twice. A
  redelivered notification MUST be recognised by its event identifier (the one recorded with the
  refund, FR-012) before any business decision is made; a different notification for an
  already-refunded order is recognised by the payment's Refunded state.
- **FR-006**: The refund MUST be durably saved before anything is announced; if the save fails,
  nothing is announced.
- **FR-007**: After a successful save the system MUST announce the refund on the
  payment-refunded channel with fields: event identifier (new), aggregate identifier (payment
  id), event time, schema version, order id, amount, currency, refund time; the message key MUST
  be the payment id.
- **FR-008**: If announcing fails after a successful save, the announcement MUST be stored with
  the existing dead-letter mechanism and delivered by the existing retry worker; no second
  mechanism is introduced.
- **FR-009**: Notifications MUST be acknowledged only after the refund (or the decision to
  ignore) has been durably handled: after the save, or after the save plus dead-lettering, or
  after deciding to ignore.
- **FR-010**: Temporary failures while reading or saving MUST be retried with exponentially
  increasing waits; if they persist, the notification MUST be logged with full context, not
  acknowledged, and retried again by the consumer with exponentially increasing waits (capped)
  without skipping to later notifications, so delivery stays at-least-once. Both consumers
  (order-placed and order-cancelled) share this behaviour.
- **FR-011**: Malformed notifications (unreadable or missing event id, order id, version or
  event time) MUST be rejected before business logic, logged as warnings, and acknowledged.
- **FR-012**: The identifier of the cancellation notification that caused a refund MUST be
  recorded with the refund for traceability.
- **FR-013**: Existing behaviour MUST NOT change — payment creation, the order-placed business
  flow, the processed/failed announcements and their contracts — with one deliberate exception:
  the shared consumer loop now retries a persistently failing notification in place instead of
  moving past it (FR-010), which fixes an at-least-once gap in the existing order-placed
  consumer (Constitution VI).

### Key Entities

- **Payment** (existing): gains the Refunded state, a refund time and the identifier of the
  cancellation notification that caused the refund. Transitions: Pending → Processed |
  Failed; Processed → Refunded. Refunded is terminal.
- **Order-cancelled notification** (inbound): event identifier, event time, aggregate
  identifier (order id), schema version, customer id, optional reason.
- **Payment-refunded announcement** (outbound): event identifier, aggregate identifier (payment
  id), event time, schema version, order id, amount, currency, refund time; keyed by payment id.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of cancellations of orders with a completed payment result in exactly one
  recorded refund and exactly one refund announcement (counting a dead-lettered announcement
  that is later delivered), except when the process crashes between recording the refund and
  announcing it (see Edge Cases).
- **SC-002**: 0 refunds are recorded or announced for orders whose payment failed, never existed
  or was already refunded, and none of these cases produces an error log or a redelivery loop.
- **SC-003**: Redelivering the same cancellation any number of times leaves exactly one refund
  and one announcement.
- **SC-004**: A refund announcement is never visible to other systems before the refund is
  durably recorded.
- **SC-005**: During a temporary messaging outage no refund announcement is lost; it is delivered
  after recovery without manual action.

## Assumptions

- **Idempotency without a processed-events table** (user decision). The cancellation's event id
  is recorded on the refunded payment (FR-012); a redelivery carrying that id is recognised as a
  duplicate before any business decision (Constitution II, dedupe by `eventId`). A different
  cancellation for the same order (the orders service issues a new event id on every publish)
  is recognised by the payment already being Refunded. Ignored cancellations (no payment,
  Failed, Pending) record nothing, and reprocessing them is a no-op.
- **Persistent consumption failures**: the user asked for the same handling as the existing
  consumer. That consumer moved past a failed message, so a later acknowledgement skipped it,
  which breaks at-least-once delivery (Constitution VI). The shared consumer loop is therefore
  changed to retry the failed message in place with capped exponential backoff (FR-010, FR-013).
  Routing a permanently failing inbound message to a dead-letter store is not part of this
  story: no inbound dead-letter mechanism exists (US-03 scoped it out). It is a follow-up
  decision.
- **Cancellation before payment exists** is ignored as instructed ("no payment exists"); a later
  charge for that order is not refunded by this feature. Closing that gap (e.g. remembering
  cancelled orders) would be a separate story.
- **Full refunds only**, using the payment's own amount and currency; partial refunds, any HTTP
  API and calls to an external payment provider are out of scope. "Refund" means recording the
  state change and announcing it.
- The refund announcement uses schema version 1 and the topic name follows the existing
  convention: `payments.payment-refunded`.
- Constitution v1.1.0 already defines these rules (Principle VI "Refund rules", `PaymentRefunded`
  → `payments.payment-refunded`, payment events carry `aggregateId` = payment id plus `orderId`);
  the saga is described in [ADR 0002](../../docs/adr/0002-saga-by-choreography.md).
- Dependency: the US-03 dead-letter table and retry worker are merged on `main`.
