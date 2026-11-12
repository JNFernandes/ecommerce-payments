# Feature Specification: Dead-Letter Retry

**Feature Branch**: `feature/US-03-dead-letter-retry`

**Created**: 2026-11-12

**Status**: Draft

**Input**: User description: "US-03 Dead-letter retry. As the Payments service, I automatically retry payment events that failed to publish to Kafka, so that no PaymentProcessed or PaymentFailed event is lost after a temporary outage." (full behaviour, scope and acceptance criteria as provided with `/speckit-specify`)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Deliver a parked payment outcome once the broker is back (Priority: P1)

When the payments system has durably recorded a payment outcome (processed or failed) but could
not announce it because the messaging platform was temporarily unavailable, the outcome is
parked as a dead letter. The payments system automatically retries announcing it, without any
manual action, so the order can still be confirmed or compensated once the outage is over.

**Why this priority**: Today a parked outcome is never delivered, so the matching order stays
pending forever after even a short outage. Automatic redelivery is the core value of this
story.

**Independent Test**: Insert a dead letter that is due for retry while the messaging platform is
available, and confirm that within about one retry interval the original announcement appears
on its destination channel, unchanged, and the dead letter is marked resolved.

**Acceptance Scenarios**:

1. **Given** a dead letter waiting for retry whose next retry time has passed, **When** the
   retry worker runs, **Then** the stored announcement is published to its original destination
   channel with its original identifier and original key, and the dead letter is marked
   resolved with the time it was resolved.
2. **Given** a dead letter waiting for retry whose next retry time is still in the future,
   **When** the retry worker runs, **Then** it is left untouched.
3. **Given** a dead letter that is already resolved or exhausted, **When** the retry worker
   runs, **Then** it is never published again.
4. **Given** dead letters that existed before this feature was deployed, **When** the feature
   is deployed, **Then** they become eligible for retry immediately.
5. **Given** the first publish of an outcome fails and a later retry succeeds, **When**
   downstream systems receive it, **Then** they receive exactly the same announcement
   (same identifier, same content) that would have been sent originally.

---

### User Story 2 - Back off from a persistently failing announcement and give up cleanly (Priority: P2)

If republishing a dead letter keeps failing, the payments system waits progressively longer
between attempts so it does not hammer an unhealthy platform. After a configured maximum number
of attempts it stops retrying that dead letter, marks it as exhausted, and logs an error with
full context so an operator can investigate.

**Why this priority**: Prevents endless tight retry loops and makes permanently undeliverable
events visible. It builds on User Story 1, which must exist first.

**Independent Test**: With the messaging platform made unavailable and a controllable clock,
drive a dead letter through repeated retries and confirm the delays grow (up to the configured
cap) and that it ends exhausted after the configured maximum attempts.

**Acceptance Scenarios**:

1. **Given** a due dead letter whose republish fails, **When** the attempt completes, **Then**
   the attempt count increases by one, the failure message and attempt time are recorded, and
   the next retry time is set using exponential backoff from the base delay, never exceeding the
   maximum delay.
2. **Given** a dead letter whose republish fails on the attempt that reaches the maximum retry
   count, **When** the attempt completes, **Then** it is marked exhausted, it is not retried
   again, and an error is logged naming the dead letter, payment, event type, attempt count and
   last error.
3. **Given** a republish fails during retry, **When** the failure is handled, **Then** no new
   dead letter is created; the existing one is updated instead.

---

### User Story 3 - Run safely on several instances at once (Priority: P3)

When more than one instance of the payments service is running, each due dead letter is picked
up by only one of them at a time, so the same announcement is not sent concurrently by two
instances and attempt bookkeeping stays correct.

**Why this priority**: Required before scaling out, but a single instance already delivers the
value of User Stories 1 and 2.

**Independent Test**: Run two claimers concurrently against the same set of due dead letters and
confirm that every dead letter is claimed by exactly one of them.

**Acceptance Scenarios**:

1. **Given** several due dead letters and two workers claiming at the same time, **When** both
   claim a batch, **Then** no dead letter is in both batches.
2. **Given** a dead letter currently being processed by one worker, **When** another worker
   looks for due dead letters, **Then** it skips that dead letter without waiting for it.

---

### Edge Cases

- **Messaging platform unavailable at startup**: the service still starts; the retry worker
  records failed attempts and backs off instead of blocking or crashing startup.
- **Shutdown during a retry cycle**: the worker stops promptly when the service is asked to
  stop; dead letters it had not finished are left in a state that is safe to retry later
  (still waiting for retry, no attempt lost or double-counted beyond at-least-once).
- **Publish succeeds but recording "resolved" fails**: the dead letter stays waiting for retry
  and is published again later; this is acceptable because delivery is at-least-once and
  downstream systems deduplicate by event identifier.
- **Database unavailable during a cycle**: the cycle is logged and skipped; the worker tries
  again on the next interval and does not crash the service.
- **More due dead letters than the batch size**: only one batch is processed per cycle; the
  rest are picked up in following cycles.
- **Dead letter with an unknown event type / no known destination**: it is treated
  like a failed attempt (logged, backed off) and eventually exhausted, never silently dropped.
- **Large backlog after a long outage**: delivery order between dead letters is not
  guaranteed; each announcement is independent and carries its own identifier.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Each dead letter MUST record its retry status (waiting for retry, resolved,
  exhausted), attempt count, next retry time, last attempt time, last error, resolved time,
  destination channel and the original message key, in addition to what it stores today.
- **FR-002**: Dead letters that exist before this feature is deployed MUST become "waiting for
  retry", due immediately, with their destination channel and key filled in from the data they
  already hold.
- **FR-003**: Newly created dead letters MUST be created as "waiting for retry", due
  immediately, with their destination channel and key recorded.
- **FR-004**: A background retry worker MUST run on a configurable interval and, on each cycle,
  pick up to a configurable batch size of dead letters that are waiting for retry and due.
- **FR-005**: The worker MUST republish the stored payload unchanged, to the stored destination,
  with the stored key. The event identifier inside the payload MUST never change.
- **FR-006**: Republishing from the worker MUST NOT create a new dead letter on failure; it MUST
  update the existing one.
- **FR-007**: On a successful republish the dead letter MUST be marked resolved with the time
  of resolution and MUST never be republished again.
- **FR-008**: On a failed republish the system MUST increase the attempt count, record the
  error and the attempt time, and schedule the next retry using exponential backoff
  (base delay × 2^(attempts − 1)), capped at a configurable maximum delay.
- **FR-009**: When the attempt count reaches the configured maximum, the dead letter MUST be
  marked exhausted, MUST NOT be retried again, and an error MUST be logged with full context
  (dead letter id, payment id, event type, destination, attempt count, last error).
- **FR-010**: A dead letter MUST be claimed by at most one worker at a time, across all running
  instances; a worker MUST skip dead letters already claimed by another instead of waiting.
- **FR-011**: All time-based decisions (due check, backoff scheduling, timestamps) MUST use a
  single replaceable clock so behaviour can be verified with a controlled clock.
- **FR-012**: The worker MUST NOT delay or prevent service startup, including when the
  messaging platform is unreachable, and MUST stop promptly and cleanly when the service shuts
  down.
- **FR-013**: A failure in one retry cycle (database or messaging) MUST be logged and MUST NOT
  stop subsequent cycles or crash the service.
- **FR-014**: Retry settings MUST be configurable with these defaults: interval 30 000 ms, batch
  size 20, base delay 5 000 ms, maximum delay 600 000 ms, maximum retries 8.
- **FR-015**: The payment business rules, the save-then-publish order of the normal flow, and
  the published event contracts MUST NOT change.
- **FR-016**: Retry of failed **inbound** messages is out of scope: the service does not store
  failed inbound messages today (it relies on redelivery of uncommitted messages), so there is
  nothing to retry.

### Key Entities

- **Dead letter**: an outbound payment announcement that could not be published after its
  payment was durably saved. Holds the payment it belongs to, the event type, the exact payload
  (including its event identifier), the destination channel, the message key, the original
  failure reason and creation time, and the retry bookkeeping from FR-001.
- **Retry status**: one of *waiting for retry* (eligible when due), *resolved* (delivered;
  terminal) or *exhausted* (gave up after maximum retries; terminal).
- **Retry settings**: interval, batch size, base delay, maximum delay, maximum retries.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: When the messaging platform is available, a due dead letter is delivered and
  marked resolved within about one retry interval (default ≤ 30 seconds plus processing time).
- **SC-002**: 100% of payment outcomes parked during a temporary outage shorter than the total
  backoff window (about 11 minutes with defaults: 5 s + 10 s + … + 320 s between the 8
  attempts) are delivered without manual action.
- **SC-003**: With two instances running concurrently, 0 dead letters are processed by both in
  the same cycle.
- **SC-004**: A permanently undeliverable dead letter is attempted exactly the configured
  maximum number of times, with each delay at least as long as the previous one and never
  longer than the maximum delay, then stops and is reported in the logs.
- **SC-005**: Service startup time is unaffected by whether the messaging platform is
  reachable.
- **SC-006**: Every redelivered announcement carries the same event identifier as the original,
  so downstream duplicate detection works for 100% of retries.

## Assumptions

- Delivery is at-least-once; downstream consumers deduplicate by `eventId`, so republishing an
  event that may already have been delivered is safe.
- The destination channel and key for existing dead letters can be derived from their stored
  event type and payment id (`PaymentProcessedEvent` → `payments.payment-processed`,
  `PaymentFailedEvent` → `payments.payment-failed`, key = payment id), which is how they were
  originally published.
- "Attempts" counts retries made by the worker; the original failed publish is not counted.
  With the defaults, delays are 5 s, 10 s, 20 s, … 320 s over 8 retries; the 10 min cap only
  takes effect with larger retry counts or base delays.
- Dead letters with an unknown event type follow the normal failure path and end exhausted.
- Ordering across different dead letters is not guaranteed.
- Out of scope: an admin API or UI for dead letters, manual replay or purge, alerting, retry of
  inbound messages, refunds (US-04).
- Design reference: [ADR 0003](../../docs/adr/0003-dead-letter-retry.md) and Constitution v1.1.0,
  Principle VI "Dead-letter retry" and "Startup & broker availability".
- Implementation constraints from the request, carried to `/speckit-plan`: EF Core migration
  for the new columns; worker as a hosted background service with settings bound from
  configuration and added to `appsettings.json`; claiming via `SELECT … FOR UPDATE SKIP LOCKED`
  inside a transaction; `TimeProvider` as the clock; a low-level publish method without the
  dead-letter fallback; tests (unit with a fake clock, Testcontainers PostgreSQL integration
  tests including two concurrent claimers, a Kafka + PostgreSQL component test where the first
  publish fails and a later one succeeds); docs (XML doc comments, `docs/dead-letter-retry.md`
  with states, settings and an example SQL INSERT, project READMEs, `CLAUDE.md`).
