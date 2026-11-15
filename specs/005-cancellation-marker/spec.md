# Feature Specification: Do Not Charge an Already-Cancelled Order

**Feature Branch**: `feature/US-05-cancellation-marker`

**Created**: 2026-11-15

**Status**: Draft

**Input**: User description: "US-05: Do not charge an order that was already cancelled. OrderCancelled and OrderPlaced travel on different topics, so a cancellation can reach Payments before the order's payment was processed … This is the same race Inventory solved with a cancellation marker." (full rules as provided with `/speckit-specify`)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Remember a cancellation that arrives before the payment (Priority: P1)

The order-placed and order-cancelled notifications travel on separate channels with no ordering
between them. When an order is cancelled quickly (for example because stock was rejected), the
cancellation can reach Payments before the order-placed notification. Today that cancellation is
ignored as "no payment exists". Instead, Payments records that the order is cancelled (a
cancellation marker), logs it, and acknowledges the notification.

**Why this priority**: Without the marker there is nothing for the later order-placed
notification to check, so no other story can work. On its own it already gives an auditable
record of early cancellations.

**Independent Test**: Deliver an order-cancelled notification for an order with no payment and
confirm that a cancellation marker exists for that order, an informational log line names the
order and event, nothing is announced, and the notification is acknowledged.

**Acceptance Scenarios**:

1. **Given** no payment and no marker exist for an order, **When** an order-cancelled
   notification for that order arrives, **Then** a cancellation marker is recorded for the
   order (with the cancelling event's identifier and time), one informational log line is
   written, nothing is announced, and the notification is acknowledged after the marker is
   durably saved.
2. **Given** a marker already exists for the order, **When** the same cancellation is
   redelivered, **Then** it is recognised by its event identifier as a duplicate, nothing
   changes, one informational log line is written, and the notification is acknowledged.
3. **Given** a marker already exists for the order, **When** a different cancellation
   notification for the same order arrives (new event identifier), **Then** no second marker is
   recorded, nothing changes, one informational log line is written, and the notification is
   acknowledged.
4. **Given** the marker cannot be durably saved because of a temporary database problem,
   **When** this is detected, **Then** the save is retried with increasing waits; if it still
   fails, the notification is not acknowledged and is retried in place, the same as any other
   persistent consumption failure.

---

### User Story 2 - Do not charge an order that is already cancelled (Priority: P1)

When an order-placed notification arrives for an order that already has a cancellation marker,
Payments does not create or evaluate a payment and announces nothing (neither processed nor
failed). Orders has already cancelled the order and expects no payment outcome. One
informational log line explains why.

**Why this priority**: This is the actual business protection: the customer is never charged
for an order that was cancelled first, with nobody left to refund it. It depends on the marker
from User Story 1.

**Independent Test**: Record a marker for an order (by delivering its cancellation first), then
deliver the order-placed notification and confirm that no payment exists for the order, no
processed or failed announcement is published, an informational log line names the order, and
the notification is acknowledged.

**Acceptance Scenarios**:

1. **Given** a marker exists for an order, **When** the order-placed notification for that
   order arrives, **Then** no payment is created, nothing is announced, one informational log
   line is written, and the notification is acknowledged.
2. **Given** the above has happened, **When** the same order-placed notification is
   redelivered, **Then** the outcome is the same: no payment, no announcement, acknowledged.
3. **Given** no marker exists, **When** an order-placed notification arrives, **Then** the
   existing behaviour is unchanged (payment created, evaluated, saved, then processed or failed
   announced).
4. **Given** a payment exists for an order, **When** an order-cancelled notification arrives,
   **Then** the existing US-04 rules apply unchanged: a Processed payment is refunded and
   announced; a Failed or already Refunded payment is ignored with a log line. No marker is
   recorded.

---

### User Story 3 - Safe when both notifications arrive at the same time (Priority: P2)

The two notifications for the same order may be handled at the same moment (different channels,
different workers or instances). Whichever outcome is durably recorded first wins, and the
other one sees it: if the payment is recorded first, the cancellation finds it and the US-04
refund rules apply; if the marker is recorded first, the order-placed notification finds it and
no payment is created.

**Why this priority**: The race is rare but its result is money taken from a customer for a
cancelled order. It hardens User Stories 1 and 2 rather than adding new behaviour.

**Independent Test**: Process an order-placed and an order-cancelled notification for the same
order concurrently, many times, and confirm that every run ends in exactly one of the two
allowed outcomes: (a) a marker and no payment, or (b) a payment that is either Refunded (if it
had been Processed) or Failed, with no marker.

**Acceptance Scenarios**:

1. **Given** both notifications for one order are handled concurrently, **When** both finish,
   **Then** the order never ends with both a marker and a payment, and never ends with a
   Processed payment that has not been refunded.
2. **Given** the losing side detects the conflict, **When** it is handled, **Then** it is
   resolved by the rule for the winner (no payment if the marker won; US-04 refund rules if the
   payment won), with no error logged as a failure and the notification acknowledged.

---

### Edge Cases

- **Order-placed first, then cancellation**: unchanged US-04 path (refund if Processed, ignore
  if Failed); no marker.
- **Cancellation first, then order-placed, then a second cancellation**: the second
  cancellation finds the marker (and no payment) and changes nothing.
- **Cancellation for an order that was failed by Payments** (the normal saga path "payment
  failed → order cancelled"): a payment exists, so no marker is recorded; US-04 ignores it.
- **Marker and a payment both exist** (only possible from data created before this feature or by
  hand): the order-placed path sees the existing payment (or the marker) and creates nothing;
  the cancellation path sees the payment and applies US-04. Not produced by this feature.
- **Malformed order-cancelled notification**: rejected before business logic, logged as a
  warning, acknowledged; no marker (unchanged).
- **Persistent database failure while recording the marker or checking it**: the notification is
  not acknowledged and is retried in place with capped increasing waits (existing consumer
  behaviour); nothing is announced.
- **Process crash after the marker is saved but before acknowledgement**: the redelivery is
  recognised as a duplicate (same event identifier) and changes nothing.
- **Markers are never removed**: an order id is never reused, so an old marker cannot block a
  new order. Expiry is out of scope.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: When an order-cancelled notification arrives and no payment exists for the order
  (identified by the notification's aggregate identifier), the system MUST durably record a
  cancellation marker for that order instead of only ignoring the notification, and MUST write
  one informational log line naming the order and the event.
- **FR-002**: The marker MUST record the order identifier, the identifier of the cancelling
  event, the time the cancellation occurred, the time it was recorded, and the optional
  cancellation reason.
- **FR-003**: At most one marker MUST exist per order; this MUST be guaranteed by the data
  store, not only by a prior check.
- **FR-004**: When an order-placed notification arrives for an order that has a marker, the
  system MUST NOT create, evaluate or save a payment, MUST NOT announce anything (neither
  processed nor failed), and MUST write one informational log line naming the order and the
  event.
- **FR-005**: When an order-cancelled notification arrives and a payment exists for the order,
  the existing US-04 behaviour MUST apply unchanged and no marker is recorded.
- **FR-006**: For the same order, recording a marker and recording a payment MUST be mutually
  exclusive even when both notifications are handled concurrently: whichever is durably
  recorded first wins, and the other notification MUST then be handled according to the
  winner (FR-004 if the marker won; FR-005 if the payment won). The system MUST never end with
  a Processed, unrefunded payment for an order that has a marker, and MUST never record a
  payment after the order's marker exists.
- **FR-007**: Losing the race described in FR-006 MUST NOT be reported as an error and MUST NOT
  cause a redelivery loop; the notification is acknowledged once it has been handled by the
  winner's rule.
- **FR-008**: Processing MUST be idempotent: a redelivered cancellation is recognised by its
  event identifier (recorded on the marker or, for refunds, on the payment) before any business
  decision; a different cancellation for an order that already has a marker changes nothing; a
  redelivered order-placed notification for a marked order again creates nothing.
- **FR-009**: Notifications MUST be acknowledged only after the marker (or the decision to skip
  or ignore) has been durably handled. Temporary database failures MUST be retried with
  increasing waits; if they persist, the notification MUST NOT be acknowledged and is retried in
  place (existing consumer behaviour).
- **FR-010**: No new announcement types or channels are introduced; existing announcement
  contracts are unchanged.
- **FR-011**: The data store change that adds the markers MUST be delivered as a versioned
  schema migration.
- **FR-012**: Existing behaviour for orders without a marker MUST NOT change.

### Key Entities

- **Order cancellation marker** (new): records that an order was cancelled before Payments saw
  its order-placed notification. Attributes: order id (unique), cancelling event id, cancelled
  time (from the notification), recorded time, optional reason. Never updated after creation;
  never removed by this feature.
- **Payment** (existing, unchanged): for any order, either a payment or a marker exists, never
  both (as produced by this feature).
- **Order-placed / order-cancelled notifications** (inbound, unchanged contracts).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 0 payments are recorded and 0 processed/failed announcements are published for
  orders whose cancellation was recorded first, across any number of redeliveries.
- **SC-002**: In repeated concurrent runs (at least 50) of an order-placed and an
  order-cancelled notification for the same order, 100% end in an allowed outcome: marker with
  no payment, or a payment that is not left Processed and unrefunded.
- **SC-003**: Every early cancellation produces exactly one marker and exactly one informational
  log line per delivery; none produces an error log or a redelivery loop.
- **SC-004**: For orders that are placed before they are cancelled, outcomes are identical to
  US-04 (same refunds, same announcements, no markers).

## Assumptions

- **Constitution**: covered by v1.2.0, Principle VI "Cancellation markers". v1.1.0 required a
  "not found" refund request to be ignored with no state change; v1.2.0 replaced that rule.
  Under v1.2.0:
  - an `OrderCancelled` with no payment records a cancellation marker;
  - an `OrderPlaced` for a marked order creates no payment and publishes nothing.
- **No outcome for marked orders**: as instructed, Orders has already cancelled the order and
  expects neither processed nor failed; publishing nothing is intended.
- **Marker idempotency**: the marker stores the cancelling event id, giving `eventId` dedupe for
  the no-payment case (Constitution II); a different cancellation for the same order is a no-op
  because of the one-marker-per-order rule.
- **Order-placed dedupe** stays as today (by order id); `eventId` dedupe for that consumer is
  the pre-existing follow-up and not part of this story.
- **Pattern**: mirrors the cancellation-marker approach used by the Inventory service for the
  same race; the mechanism for FR-006 is chosen in the plan.
- **Out of scope**: expiring old markers, any HTTP API, partial refunds.
- This removes the README "Known limitations" entry about a cancellation arriving before its
  payment exists.
