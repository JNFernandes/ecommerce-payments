# Feature Specification: Inbound Dead-Letter for Messages That Keep Failing

**Feature Branch**: `feature/US-06-inbound-dead-letter`

**Created**: 2026-11-15

**Status**: Draft

**Input**: User description: "start us-06". The scope was agreed in the US-05 analysis
(finding D1). After N consumer retries, a message that keeps failing is stored in an inbound
dead-letter table and its offset is committed, as Inventory does with `consumed_dead_letters`.
The constitution stays as is (v1.2.0, Principle VI: "Failed message processing MUST be retried
with exponential backoff before being routed to a dead-letter table/topic").

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A message that keeps failing no longer blocks its partition (Priority: P1)

Today, when handling an order-placed or order-cancelled notification keeps failing, the
consumer retries the same notification forever. Every later notification on that partition
waits behind it, so payments and refunds for other orders stop.

After a configured number of failed attempts, the payments system now stores the notification
exactly as received, together with where it came from and why it failed. It logs an error and
acknowledges the notification, so later notifications are processed again.

**Why this priority**: one bad notification can currently halt payment processing for every
order on that partition until someone intervenes by hand. This is the gap carried over from
US-04 (Constitution VI).

**Independent Test**: make handling of one notification always fail, then send it followed by
a valid notification. Confirm three things:
- the failing one is stored after the configured number of attempts, with its channel,
  position, key, content, error and attempt count;
- an error is logged;
- the valid notification behind it is then processed normally.

**Acceptance Scenarios**:

1. **Given** handling of a notification fails on every attempt, **When** it has failed the
   configured maximum number of times, **Then** it is stored in the inbound dead-letter store
   with the following, and only then acknowledged:
   - the channel (topic), partition and position (offset);
   - the message key and the raw content exactly as received;
   - the last error (its type and message);
   - the number of attempts;
   - the time it was stored.
2. **Given** the notification has been stored and acknowledged, **When** the next notification
   on the same partition arrives, **Then** it is processed normally.
3. **Given** handling of a notification fails a few times and then succeeds before the
   maximum, **When** it succeeds, **Then** nothing is stored and it is acknowledged as today.
4. **Given** the maximum is reached but the notification cannot be stored (for example the
   database is unavailable), **When** this is detected, **Then**:
   - it is **not** acknowledged;
   - the error is logged;
   - the consumer keeps retrying the notification itself, with capped increasing waits.

   If a later attempt succeeds, the notification is handled normally. If it fails again, the
   consumer tries to store it again. Nothing is ever lost.
5. **Given** the same notification is stored twice for the same channel, partition and
   position (for example after a crash between storing and acknowledging), **When** the
   second store happens, **Then** one record is kept and the notification is acknowledged.

---

### User Story 2 - Malformed notifications are kept for review instead of dropped (Priority: P2)

Today a malformed notification is logged as a warning and acknowledged, so its content survives
only in the logs. Such notifications include unreadable content and missing or invalid
required fields. Retrying them can never succeed.

They are now stored in the same inbound dead-letter store on the first attempt, with the
validation error. They are still acknowledged immediately and never retried.

**Why this priority**: it keeps a durable record of every notification Payments did not act on,
in one place. It builds on the store from User Story 1.

**Independent Test**: send a malformed order-placed notification and a malformed
order-cancelled notification. Confirm that each is stored once with attempt count 1 and the
validation error, that it is acknowledged, and that nothing else changes.

**Acceptance Scenarios**:

1. **Given** a notification that cannot be read or fails envelope validation, **When** it
   arrives, **Then** it is stored with attempt count 1 and the validation error, a warning is
   logged, and it is acknowledged without any retry.
2. **Given** a malformed notification cannot be stored because the database is unavailable,
   **When** this is detected, **Then** it is not acknowledged and is retried like any other
   failure, so it is not lost.

---

### User Story 3 - Operators can find and replay dead-lettered notifications (Priority: P3)

An operator can list the stored notifications, see why each failed, and replay one by sending
its stored content back to its original channel. Handling is idempotent (US-01 to US-05), so a
replay of a notification that was in fact already handled changes nothing.

**Why this priority**: this is needed to recover from the underlying problem once it is fixed.
It needs only documentation and data; no new runtime behaviour.

**Independent Test**: follow the documented procedure to replay a stored order-placed
notification once its cause is fixed. Confirm the payment is then processed exactly once.

**Acceptance Scenarios**:

1. **Given** stored notifications exist, **When** an operator follows the documented query,
   **Then** they see each notification's channel, partition, position, key, content, error,
   attempts and stored time, newest first.
2. **Given** a stored order-placed notification whose cause has been fixed, **When** the
   operator replays it by the documented procedure, **Then** it is handled like any new
   notification, with the same idempotency guarantees.

---

### Edge Cases

- **Database down for longer than the retry budget**: handling fails, and so does storing. The
  notification is never acknowledged and keeps being retried with capped waits. When the
  database returns, the next attempt handles it normally; it is stored only if handling still
  fails. The partition stays blocked for as long as the database is down, which is correct:
  nothing behind it could be handled either.
- **Process restart during retries**: the attempt count is not persisted. After a restart the
  notification is redelivered and counting starts again. The worst case is extra retries,
  never a lost or skipped notification.
- **Shutdown while waiting between attempts**: this is not counted as a failed attempt.
  Nothing is stored and nothing is acknowledged.
- **Dead-lettering an order-placed notification for an order that is later cancelled**: the
  cancellation finds no payment and records a cancellation marker (US-05). A later replay of
  the stored order-placed notification is therefore not charged.
- **Dead-lettering an order-cancelled notification for a Processed payment**: the payment stays
  Processed until the stored notification is replayed. This is reported by the Error log and by
  the stored row, and recovered by replay (User Story 3).
- **Very large or non-text content**: stored as received, up to whatever the transport delivers;
  no transformation.
- **Same position stored twice**: kept once (User Story 1, scenario 5).
- **Ordering**: a stored notification is skipped while later ones on the partition continue.
  The saga tolerates this because each event is idempotent and order-independent (US-05
  markers), and replay restores the skipped event.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST count consecutive failed handling attempts of the current
  notification and keep retrying it in place with capped, increasing waits (unchanged from
  US-04) until it succeeds or the configured maximum number of attempts is reached.
- **FR-002**: When the maximum is reached, the system MUST durably store the notification as
  received:
  - channel, partition, position and key;
  - raw content;
  - last error (type and message);
  - attempt count;
  - stored time.

  Only after the store succeeds MUST it acknowledge the notification.
- **FR-003**: If the store fails, the system MUST NOT acknowledge the notification. It MUST log
  the failure and continue retrying the notification with capped waits. Each retry first
  attempts normal handling again.
- **FR-004**: At most one stored record MAY exist per channel, partition and position. Storing
  the same notification again MUST succeed without creating a duplicate.
- **FR-005**: Each notification stored after retries MUST produce one Error-level log entry
  naming the channel, partition, position, attempt count and error.
- **FR-006**: Malformed notifications (unreadable, or failing envelope validation) MUST be
  stored on the first attempt with attempt count 1 and the validation error, logged as a
  warning, and acknowledged without retry. If the store fails, FR-003 applies.
- **FR-007**: The maximum number of attempts MUST be configurable and validated as a positive
  integer at startup.
- **FR-008**: Behaviour for notifications that succeed before the maximum MUST NOT change:
  same handling, same acknowledgement and nothing stored.
- **FR-009**: Both inbound consumers (order-placed and order-cancelled) MUST share this
  behaviour.
- **FR-010**: The data store change MUST be delivered as a versioned schema migration.
- **FR-011**: The documentation MUST describe how to list stored notifications and how to replay
  one to its original channel.
- **FR-012**: No new announcement types or channels are introduced. Stored notifications are not
  replayed automatically.

### Key Entities

- **Inbound dead letter** (new): a notification Payments could not handle, kept as received.
  - Attributes: id, channel (topic), partition, position (offset), message key (optional), raw
    content (optional), error, attempt count (at least 1), stored time.
  - Unique by channel, partition and position.
  - Never updated or removed by the system.
- **Outbound dead letter** (existing, `payment_dead_letters`): unchanged. It holds events
  Payments failed to *publish*. The two stores are separate.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A notification that always fails stops blocking its partition after the
  configured maximum number of attempts plus their waits. With the default settings (see
  Assumptions) this is about 15 seconds. The next notification on the partition is then
  processed.
- **SC-002**: 0 notifications are lost. Every notification is either handled and acknowledged,
  or stored and then acknowledged, or still being retried. This holds while the database is
  unavailable.
- **SC-003**: 100% of malformed notifications are stored exactly once, with no retries.
- **SC-004**: Notifications that succeed within the maximum are handled exactly as before, with
  no stored records.
- **SC-005**: An operator can replay a stored notification using only the documented procedure.

## Assumptions

- **Default maximum attempts: 5.** With the existing backoff (1 s doubling, capped at 60 s), the
  waits before the fifth attempt total about 1 + 2 + 4 + 8 = 15 s.
- **Malformed notifications are now stored** (User Story 2). They were only logged before; they
  are still acknowledged without retry. Inventory stores them the same way.
- **No automatic replay, no expiry, no status tracking** on stored records. Replay is a
  documented manual procedure that relies on the existing idempotency.
- **Separate from the outbound dead-letter table.** `payment_dead_letters` and its retry worker
  (US-03) are unchanged.
- **The attempt count lives in memory**, per consumer and per current notification. Persisting
  it is unnecessary because a restart only causes extra retries.
- **Pattern**: mirrors Inventory's `consumed_dead_letters` (unique topic, partition and offset;
  raw payload; error; attempts; failed time).
- Constitution v1.2.0, Principle VI already requires this behaviour, and Principle I already
  assigns routing to retry or dead-letter to the Consumer layer. No amendment is needed.
- Out of scope: any HTTP API, automatic replay, alerting and metrics beyond logs, and expiry of
  stored records.
