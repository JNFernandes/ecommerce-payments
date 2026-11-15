# Feature Specification: Transactional Outbox for Payment Events

**Feature Branch**: `feature/US-07-transactional-outbox`

**Created**: 2026-11-15

**Status**: Draft

**Input**: User description: "for the outbox (US-07) -> start this". The scope is the follow-up
recorded since US-04: "If the process crashes after a refund (or payment) is saved but before
its event is published or dead-lettered, that event is lost — there is no transactional
outbox."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A recorded payment outcome is always announced, even after a crash (Priority: P1)

Today the payments system saves a payment outcome (processed, failed or refunded) and then
announces it. If publishing fails, the announcement is parked in the dead-letter table and
retried. If the process stops between the save and the announcement (crash, kill, container
restart), the outcome is saved but never announced. Orders then waits for ever for a payment
result, and the saga for that order stalls.

With this feature, the announcement is recorded **together with** the outcome, in the same
all-or-nothing save. An outcome is never saved without its announcement. A background delivery
process then announces every recorded announcement, retrying until it succeeds.

**Why this priority**: it removes the last way the service can silently lose an event. The
other gaps (US-03 publish failures, US-05 races, US-06 failing messages) are already closed.

**Independent Test**: arrange for the process to stop right after an outcome is saved and
before anything is announced. Restart it, and confirm the announcement is delivered once
without manual action.

**Acceptance Scenarios**:

1. **Given** an order-placed notification is handled, **When** the payment outcome is saved,
   **Then** its announcement (processed or failed) is saved in the same all-or-nothing save.
   Either both exist or neither does.
2. **Given** an order-cancelled notification refunds a payment, **When** the refund is saved,
   **Then** its refund announcement is saved in the same all-or-nothing save.
3. **Given** an outcome and its announcement were saved, **When** the process stops before
   announcing, **Then** after restart the announcement is delivered automatically, with the
   same event identifier, key, channel and content it was saved with.
4. **Given** the save of an outcome fails, **When** this is detected, **Then** no
   announcement is recorded or delivered (unchanged write-flow rule).
5. **Given** an announcement cannot be delivered because the messaging system is unavailable,
   **When** delivery is retried, **Then** it keeps being retried with increasing waits until it
   succeeds or reaches the configured maximum. Reaching the maximum marks it as needing
   attention, as US-03 does today.

---

### User Story 2 - Announcements are prompt in normal operation (Priority: P1)

Moving announcements through a stored queue must not make the system noticeably slower when
everything is healthy. Other services must see a payment outcome about as quickly as they do
today.

**Why this priority**: Orders, Inventory and Notifications react to these announcements. A
large added delay would slow every order.

**Independent Test**: process an order with everything healthy, and measure the time from the
order-placed notification being handled to the payment announcement being visible.

**Acceptance Scenarios**:

1. **Given** the messaging system is healthy, **When** a payment outcome is saved, **Then**
   its announcement is visible to other systems within 2 seconds.
2. **Given** a burst of outcomes, **When** they are saved, **Then** each announcement is
   delivered within the same bound, and none is delayed behind an unrelated failing one.

---

### User Story 3 - Announcements for one payment arrive in the order they happened (Priority: P2)

A payment can produce two announcements: processed, then refunded. Today, if the processed
announcement is parked for retry while the refund is announced at once, Orders can see
"refunded" before "processed". Announcements for the same payment are now delivered in the
order they were recorded.

**Why this priority**: an out-of-order "refunded before processed" confuses downstream
services, but it needs a publish failure plus a fast cancellation to happen. It refines US1.

**Independent Test**: make the processed announcement for a payment fail to deliver, then
refund the payment. Confirm that the refund announcement is not delivered before the processed
one, and that both are delivered in order once delivery recovers.

**Acceptance Scenarios**:

1. **Given** a payment has an undelivered earlier announcement, **When** a later announcement
   for the same payment is recorded, **Then** the later one is not delivered until the earlier
   one has been delivered.
2. **Given** an earlier announcement for a payment has reached the retry maximum, **When** a
   later one exists for the same payment, **Then** the later one also waits, and both are
   reported as needing attention.
3. **Given** announcements for different payments, **When** one payment's announcement is
   failing, **Then** other payments' announcements are not held back.

---

### Edge Cases

- **Crash after an announcement was delivered but before it was marked delivered**: it is
  delivered again after restart, with the same event identifier. Delivery is at-least-once, and
  downstream consumers deduplicate by event identifier (Constitution VI).
- **Several service instances**: each recorded announcement is delivered by one instance at a
  time, and two instances never deliver the same one concurrently (same guarantee as US-03).
- **Messaging system unavailable at startup**: the service starts and records outcomes. Their
  announcements are delivered once messaging is back (Constitution VI, startup independence).
- **Announcements already parked in the US-03 dead-letter table when this ships**: they are
  still delivered. None is lost or duplicated by the switch-over.
- **Ignored notifications** (US-04 ignore cases, US-05 cancellation markers, US-06 dead
  letters): they record no announcement, unchanged.
- **Event content**: an announcement is delivered exactly as recorded. Its content, event
  identifier and key never change between recording and delivery.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Every payment announcement (processed, failed, refunded) MUST be durably
  recorded in the same all-or-nothing save as the payment state change that produced it.
- **FR-002**: The request handlers MUST NOT publish to the messaging system directly. A
  background delivery process MUST deliver every recorded announcement.
- **FR-003**: The delivery process MUST deliver each announcement unchanged: the same event
  identifier, key (payment id), channel and content as recorded.
- **FR-004**: Delivery MUST be retried with exponentially increasing waits up to a configured
  maximum. An announcement that reaches the maximum MUST be marked as needing attention and
  logged as an error. Behaviour and settings match US-03 where applicable.
- **FR-005**: Each announcement MUST be delivered by at most one service instance at a time.
- **FR-006**: In healthy conditions an announcement MUST be visible to other systems within 2
  seconds of being recorded.
- **FR-007**: Announcements for the same payment MUST be delivered in the order they were
  recorded. An announcement failing for one payment MUST NOT delay other payments'
  announcements.
- **FR-008**: The inbound notification MUST be acknowledged once the state change and its
  announcement have been saved, without waiting for delivery.
- **FR-009**: The existing outbound dead-letter store and its retry process (US-03) MUST
  become the outbox and its delivery process. There MUST remain a single outbound mechanism
  (user decision, Q1 option A). Announcements already waiting in it when this ships MUST still
  be delivered, with no loss and no duplicate. Announcements already delivered or marked as
  needing attention MUST keep their status and history.
- **FR-010**: Startup MUST NOT depend on the messaging system being reachable.
- **FR-011**: Announcement contracts (topics, payload fields, keys) MUST NOT change.
- **FR-012**: The schema change MUST be delivered as a versioned migration.
- **FR-013**: Delivered announcements MUST remain queryable for support: when each was
  recorded, when it was delivered, its retry count and its last error.

### Key Entities

- **Outbox entry (pending announcement)**: one payment announcement waiting for delivery or
  already delivered.
  - Attributes: event identifier, payment id (key), channel (topic), event type, serialized
    content, status (pending, delivered, needs attention), retry count, next attempt time, last
    error, recorded time, delivered time.
  - Ordered per payment by recorded time.
- **Payment** (existing): unchanged. Each state change that is announced creates exactly one
  outbox entry in the same save.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 0 payment outcomes are left unannounced after a process stop at any point in
  handling, verified by killing the process between the save and the delivery.
- **SC-002**: In healthy conditions, 95% of announcements are visible to other systems within
  1 second of the outcome being saved, and 100% within 2 seconds.
- **SC-003**: Announcements for one payment are never delivered out of recorded order.
- **SC-004**: During a messaging outage of any length, no announcement is lost. Every one is
  delivered after recovery or marked as needing attention after the configured maximum, with
  no manual action needed for the delivered ones.
- **SC-005**: Downstream consumers see no contract change: the same topics, fields and keys.

## Assumptions

- **Constitution**: covered by v1.3.0. Its Principle II write flow uses the outbox, and
  Principle VI's "Outbox relay" section requires a single outbound store, per-payment ordering
  and the 2-second healthy-path bound.
- **At-least-once delivery** stays the contract. A crash after delivering but before marking
  can produce one duplicate with the same event identifier, which consumers already handle.
- **Delivered entries are kept** for support (FR-013). Cleaning them up is out of scope, like
  US-05 markers and US-06 dead letters.
- **Retry settings** reuse the US-03 defaults (base 5 s, cap 10 min, 8 retries) unless the plan
  shows a reason to change them. The 2-second healthy-path bound (FR-006) is separate from the
  retry waits.
- Out of scope: any HTTP API, changes to event contracts, cleanup of old entries, and the
  `OrderPlaced` `eventId` dedupe follow-up.
