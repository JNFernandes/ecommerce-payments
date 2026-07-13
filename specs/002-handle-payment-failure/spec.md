# Feature Specification: Handle Payment Failure

**Feature Branch**: `feature/US-02-handle-payment-failure`

**Created**: 2026-07-09

**Status**: Draft

**Input**: User description: "US-02: Handle payment failure — As the system, I want to publish PaymentFailed when payment cannot be processed, so that the order can be compensated."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Record and announce a payment that cannot be completed (Priority: P1)

When the payments system determines that a payment cannot be completed for a placed order, it
records that outcome — with a reason — and announces it, so the rest of the platform (in
particular, the order itself) can react and compensate (e.g., cancel the order) instead of the
order being silently left in limbo with no signal that payment never happened.

**Why this priority**: This is the core value of this feature — without it, a payment that
cannot go through leaves the order unpaid with no way for anything downstream to find out and
react. This is the direct counterpart to the "process payment" feature's happy path, for the
case where payment does not succeed.

**Independent Test**: Can be fully tested by sending an order-placement notification whose amount
exceeds the configured maximum threshold and confirming that (a) a payment record is created and
marked as failed, with a reason referencing the threshold, and (b) a payment-failed notification
is announced afterward.

**Acceptance Scenarios**:

1. **Given** an order-placement notification whose payment cannot be completed, **When** the
   payments system evaluates it, **Then** a payment record is marked failed, with a reason
   describing why.
2. **Given** the failed payment outcome has been recorded, **When** the system saves that
   outcome, **Then** the failed payment record MUST be durably saved before anything is
   announced to other systems.
3. **Given** the failed payment record has been durably saved, **When** the save is confirmed,
   **Then** a payment-failed notification MUST be announced so other systems can react (e.g.,
   compensate or cancel the order).
4. **Given** the system is temporarily unable to durably save a failed payment outcome, **When**
   this is detected, **Then** no payment-failed notification is announced, the issue is logged
   with full context, and the original order-placement notification is retried — the same
   technical-failure handling as the successful-payment path, not a business outcome.
5. **Given** a payment legitimately cannot be completed, **When** this is recorded, **Then** it
   MUST NOT be logged, alerted, or otherwise treated as a system error — it is a valid, expected
   business outcome, distinct from a technical/infrastructure failure.

---

### User Story 2 - Never double-announce a payment failure on duplicate notifications (Priority: P2)

If the same order-placement notification is delivered more than once after a payment failure has
already been recorded for that order, the payments system recognizes this and does not record a
second failure or announce a second payment-failed notification.

**Why this priority**: Without this safeguard, the rest of the platform could receive duplicate
failure signals and attempt to compensate the same order more than once. This matters, but the
core recording-and-announcing behavior (User Story 1) delivers value first.

**Independent Test**: Can be fully tested by sending the identical order-placement notification
twice, where the payment cannot be completed, and confirming that only one failed payment record
exists and only one payment-failed notification was announced.

**Acceptance Scenarios**:

1. **Given** an order-placement notification has already resulted in a recorded payment failure,
   **When** the exact same notification is received again, **Then** no second failed payment
   record is created and no second payment-failed notification is announced.

---

### Edge Cases

- What happens when a redelivered order-placement notification is evaluated after that order
  already has *any* payment outcome recorded — whether it previously succeeded or previously
  failed? It MUST be recognized as already handled and skipped without side effects; the outcome
  is not re-evaluated on redelivery.
- What happens when the system is temporarily unable to durably save a failed payment outcome?
  See User Story 1, Acceptance Scenario 4 — it is retried without announcing a false outcome,
  identically to how the same situation is handled on the successful-payment path.
- How is a legitimate business failure distinguished from a technical/infrastructure failure
  (e.g., storage temporarily unreachable)? Only a business failure results in a payment-failed
  notification; a technical failure results in a retry with nothing announced yet (see User
  Story 1, Acceptance Scenarios 4-5).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST evaluate, for a valid order-placement notification, whether the
  represented payment can be completed. A payment cannot be completed when its amount exceeds a
  configurable maximum threshold (orders above this limit require manual review and are recorded
  as failed rather than charged automatically). The threshold is a business-configurable value,
  not a fixed constant baked into this requirement.
- **FR-002**: System MUST record a payment as failed, capturing a reason, when it determines
  that payment cannot be completed.
- **FR-003**: System MUST durably save the failed payment record before announcing the failure.
- **FR-004**: System MUST announce a payment-failed notification only after the failed payment
  record has been durably saved.
- **FR-005**: System MUST NOT announce a payment-failed notification if the failed payment
  record could not be durably saved; the failure MUST be logged with enough context to diagnose
  it, and the order-placement notification MUST be retried rather than discarded.
- **FR-006**: System MUST NOT create a duplicate failed payment record or announce a duplicate
  payment-failed notification when the same order-placement notification is received more than
  once, or when an order already has any payment outcome recorded.
- **FR-007**: The payment-failed notification MUST contain enough information for other systems
  to identify the payment, the originating order, the reason processing could not complete, and
  when the failure was recorded.
- **FR-008**: A business payment failure MUST NOT be logged, alerted, or otherwise surfaced as a
  system error — it is a valid, expected outcome, handled distinctly from technical/
  infrastructure failures.

### Key Entities *(include if feature involves data)*

- **Payment**: The same record introduced by the "process payment" feature, now also reachable
  in a "failed" state. When failed, it additionally carries a reason describing why the payment
  could not be completed.
- **Payment-Failed Notification**: The outgoing signal that a payment could not be completed.
  Key attributes: a unique identifier, when it was announced, the payment/order it refers to,
  the reason processing failed, and when the failure was recorded.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of payments that cannot be completed result in exactly one failed payment
  record being created, with a reason captured.
- **SC-002**: 100% of announced payment-failed notifications correspond to a failed payment
  record that was already durably saved — a failure is never announced without matching durable
  state.
- **SC-003**: 0 duplicate failure records or duplicate payment-failed notifications occur when
  the same order-placement notification is redelivered any number of times.
- **SC-004**: 0 legitimate business payment failures are logged, alerted, or otherwise surfaced
  as system errors.
- **SC-005**: When a temporary recording failure occurs while saving a failed payment outcome,
  processing automatically recovers and completes successfully once the underlying issue
  clears, with no manual intervention required.

## Assumptions

- This feature does not integrate with an external payment gateway/processor — consistent with
  the companion "process payment" feature's own scope boundary. "Cannot be completed" is
  evaluated using a single business rule owned by this system: the order amount exceeds a
  configurable maximum threshold. The specific threshold value is an operational/business
  configuration decision, not fixed by this specification.
- This one rule is expected to be the first of potentially several failure conditions as the
  platform evolves (e.g., once a real payment gateway or fraud/risk service exists); the
  mechanism (evaluate → record → announce) is built to accommodate additional reasons later
  without being re-architected, but only the amount-threshold rule is in scope now.
- The retry-on-technical-failure and duplicate-detection mechanisms already established for
  successful payment processing are reused for the failure path, since both paths share the same
  underlying "one outcome per order" record and durability guarantees.
- The specific set of human-readable failure reasons is expected to evolve over time; this
  feature only requires that some reason is captured and propagated, not a fixed, exhaustive
  enumeration of reasons.
- Compensating the order (e.g., cancellation) is performed by the order-placement system itself
  upon receiving the payment-failed notification; this feature's responsibility ends at durably
  recording and announcing the failure.
