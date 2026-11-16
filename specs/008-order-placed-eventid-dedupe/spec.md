# Feature Specification: Recognise Redelivered OrderPlaced by Event Identifier

**Feature Branch**: `feature/US-08-order-placed-eventid-dedupe`

**Created**: 2026-11-16

**Status**: Draft

**Input**: User description: "yes you can start". The scope is the follow-up recorded since
US-04: "`OrderPlacedConsumer` deduplicates by order id rather than by `eventId`." Constitution
v1.3.0, Principle II, says: "Consumers MUST deduplicate redelivered messages by the inbound
event's `eventId`, checked by the Service/Repository before domain execution."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A redelivered order-placed notification is recognised as a duplicate by its event identifier (Priority: P1)

Kafka can deliver the same order-placed notification more than once, for example after a
consumer restart or a rebalance. Today the payments system ignores the second copy because a
payment already exists for the order. The outcome is correct, but:
- the service never checks the notification's own identifier, as the constitution requires;
- the redelivery is skipped silently.

With this feature, before any business decision, the payments system compares the
notification's event identifier with the one recorded on the order's payment. When they match,
it recognises the notification as a duplicate delivery, writes one informational log line, and
acknowledges it. Nothing else changes.

**Why this priority**: it brings the order-placed consumer in line with the constitution and
with the order-cancelled consumer (US-04). It also makes redeliveries visible in the logs.

**Independent Test**: deliver the same order-placed notification twice. Confirm:
- there is exactly one payment and one recorded announcement;
- the second delivery writes one informational log line saying it is a duplicate delivery of
  that event;
- the second delivery is acknowledged.

**Acceptance Scenarios**:

1. **Given** a payment created by order-placed notification E, **When** E is delivered again,
   **Then**:
   - it is recognised by its event identifier before any business decision;
   - nothing is created or recorded;
   - one informational log line names the order, the event and the payment as a duplicate
     delivery;
   - the notification is acknowledged.
2. **Given** a payment created by notification E, **When** a different order-placed
   notification E′ arrives for the same order (Orders re-published it with a new event
   identifier), **Then**:
   - it is recognised by the existing payment for the order, as today;
   - nothing is created or recorded;
   - one informational log line says a payment already exists for the order, created by E;
   - the notification is acknowledged.
3. **Given** no payment exists for the order, **When** an order-placed notification arrives,
   **Then** behaviour is unchanged: with no cancellation marker a payment is created, and with a
   marker it is skipped (US-05).

---

### Edge Cases

- **Order cancelled first (marker, no payment), then the order-placed notification is
  redelivered**: there is no payment to compare against, so the marker rule applies, unchanged.
  The order is not charged, and the same informational line is written.
- **Payments created before this feature**: they already carry the identifier of the
  notification that created them, so the comparison works for existing data.
- **Concurrent deliveries of the same notification**: these are already serialised by the
  per-order lock (US-05). The second one sees the payment and is recognised as a duplicate.
- **Malformed notification**: rejected before this check, as today (US-06).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Before any business decision on an order-placed notification, the system MUST
  compare its event identifier with the event identifier recorded on the order's payment, if
  one exists. A match MUST be treated as a duplicate delivery.
- **FR-002**: A duplicate delivery MUST NOT create, change, or record anything. It MUST write
  exactly one informational log line naming the order, the event identifier and the payment,
  and the notification MUST be acknowledged.
- **FR-003**: An order-placed notification with a different event identifier for an order that
  already has a payment MUST still be ignored. It MUST write one informational log line naming
  the existing payment and the event that created it.
- **FR-004**: Behaviour for orders without a payment (creation, evaluation, cancellation
  marker) MUST NOT change.
- **FR-005**: No new data, migration, announcement or contract change is introduced.

### Key Entities

- **Payment** (existing, unchanged): already records the identifier of the order-placed
  notification that created it.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Delivering the same order-placed notification any number of times leaves exactly
  one payment and one recorded announcement. Every repeat produces one "duplicate delivery" log
  line and no error.
- **SC-002**: 100% of order-placed redeliveries are recognised by event identifier when the
  payment was created by that same notification.
- **SC-003**: Orders without a payment behave exactly as before (existing tests unchanged).

## Assumptions

- Constitution v1.3.0, Principle II already requires this; no amendment is needed.
- Order id stays the idempotency key for payment creation (Principle II: "At most one Payment
  exists per order"). The event identifier check is an additional, earlier check, and does not
  replace it.
- Out of scope: any inbox or processed-events table (the identifier is already stored on the
  payment), changes to other consumers, and retention.
- The README's known limitation and the `CLAUDE.md` note about this gap are removed when this
  is implemented.
