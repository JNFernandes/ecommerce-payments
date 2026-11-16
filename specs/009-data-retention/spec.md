# Feature Specification: Data Retention for Outbox, Dead Letters and Cancellation Markers

**Feature Branch**: `feature/US-09-data-retention`

**Created**: 2026-11-16

**Status**: Draft

**Input**: "we can do this to finish up the service". This is the follow-up recorded in US-05,
US-06 and US-07: published outbox entries, inbound dead letters and cancellation markers are
never removed. The service needs a retention policy and a background job that purges records
once they are older than the retention period.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Old published announcements are purged automatically (Priority: P1)

Every payment outcome leaves one outbox entry. Once it has been published, it is kept only for
support. The service now deletes published entries older than a configured retention period.
Entries that still matter are never deleted:
- entries waiting to be published;
- entries that gave up and need attention.

**Why this priority**: the outbox grows with every payment, so it is the table that grows
fastest. It is also the safest to purge, because a published entry has no further role.

**Independent Test**: create published entries both inside and outside the retention period,
plus pending and exhausted entries of any age. Run a purge, then check:
- only the published entries older than the retention period are gone;
- every other entry is unchanged.

**Acceptance Scenarios**:

1. **Given** a published entry whose publish time is older than the outbox retention period,
   **When** the purge runs, **Then** the entry is deleted.
2. **Given** a published entry inside the retention period, **When** the purge runs, **Then**
   it is kept.
3. **Given** a pending or exhausted entry of any age, **When** the purge runs, **Then** it is
   kept.
4. **Given** the outbox retention is switched off, **When** the purge runs, **Then** no outbox
   entry is deleted.

---

### User Story 2 - Old inbound dead letters and cancellation markers are purged (Priority: P2)

The same policy applies to two more stores:
- **Inbound dead letters** (US-06) are kept long enough for an operator to investigate and
  replay them, and are then deleted.
- **Cancellation markers** (US-05) are kept long enough that the order-placed notification they
  guard can no longer arrive, and are then deleted.

Each store has its own retention period, and each can be switched off.

**Why this priority**: both tables grow only with incidents or early cancellations, so they
grow slowly. Markers also need a long, deliberate period: purging one too early would let a
very late order-placed notification charge a cancelled order.

**Independent Test**: create dead letters and markers both inside and outside their retention
periods. Run a purge, then check that only the old ones are deleted.

**Acceptance Scenarios**:

1. **Given** an inbound dead letter stored longer ago than the dead-letter retention period,
   **When** the purge runs, **Then** it is deleted. Newer ones are kept.
2. **Given** a cancellation marker recorded longer ago than the marker retention period,
   **When** the purge runs, **Then** it is deleted. Newer ones are kept.
3. **Given** a store whose retention is switched off, **When** the purge runs, **Then**
   nothing in that store is deleted.

---

### User Story 3 - Purging is safe to run continuously, on any number of instances (Priority: P2)

The purge runs in the background on a fixed interval. It removes large backlogs in small
batches, so it never holds long locks or delays payment processing. It can run on several
instances at once. Every run logs how many records were deleted from each store.

**Why this priority**: a purge that blocked payment processing, or failed when two instances
ran it, would make things worse than the problem it solves.

**Independent Test**: create more expired records than one batch holds, then run the purge on
two instances at the same time. Check that all expired records are deleted, that nothing else
is, and that no errors occur.

**Acceptance Scenarios**:

1. **Given** more expired records than one batch, **When** a purge run completes, **Then** all
   of them are deleted, in several small batches.
2. **Given** two instances purging at the same moment, **When** both finish, **Then** every
   expired record is deleted exactly once, nothing else is deleted, and neither instance fails.
3. **Given** the database is unavailable, **When** a purge run fails, **Then** it is logged and
   retried on the next interval. Startup and payment processing are not affected.
4. **Given** a purge run completes, **When** it is logged, **Then** the log line gives the
   count deleted from each store, at Information level when anything was deleted.

---

### Edge Cases

- **A published entry is kept while a later entry for the same payment is pending or
  exhausted**: the per-payment ordering check (US-07) treats a missing earlier entry the same
  as a published one, so purging published entries never blocks or reorders later ones.
- **Exhausted entries are never purged**, whatever their age. They need an operator.
- **A very late order-placed notification for a cancelled order whose marker was purged**: it
  would be charged. The marker retention default is therefore long, and the docs state this
  trade-off.
- **A dead letter is deleted before anyone replays it**: its payload is gone. The default
  dead-letter retention is long, and the docs advise replaying or exporting rows before they
  expire.
- **Clock and time zones**: ages are measured from the stored timestamps, which are in UTC.
- **Retention changed while running**: the new value is used from the next run.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST periodically delete published outbox entries whose publish time
  is older than the outbox retention period. It MUST NOT delete pending or exhausted entries,
  whatever their age.
- **FR-002**: The system MUST periodically delete inbound dead letters whose stored time is
  older than the dead-letter retention period.
- **FR-003**: The system MUST periodically delete cancellation markers whose recorded time is
  older than the marker retention period.
- **FR-004**: Each of the three retention periods MUST be configurable on its own and MUST be
  able to be switched off (keep forever). The run interval and batch size MUST be
  configurable. Invalid values MUST stop the service at startup.
- **FR-005**: Deletion MUST happen in bounded batches, so no single statement deletes an
  unbounded number of rows or holds locks for long.
- **FR-006**: Purging MUST be safe to run on several instances at the same time, with no
  errors and no record deleted that is not expired.
- **FR-007**: A failed purge run MUST be logged and retried on the next interval. It MUST NOT
  crash the service, delay startup, or affect payment processing.
- **FR-008**: Each run MUST log how many records were deleted from each store. The level is
  Information when anything was deleted and Debug otherwise.
- **FR-009**: Purging MUST NOT change the outcome of any payment flow (US-01 to US-08). The
  only accepted exception is a late order-placed notification arriving after its order's
  marker has been purged, as covered in Edge Cases.
- **FR-010**: The documentation MUST describe each retention setting, its default, what is
  never purged, and the risk of purging markers.

### Key Entities

- **Retention policy** (configuration): run interval, batch size, and a retention period per
  store: outbox (published only), inbound dead letters, cancellation markers. Each period can
  be switched off.
- Existing stores, unchanged in shape: published outbox entries, inbound dead letters and
  cancellation markers.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a purge run, 0 expired records of an enabled store remain, and 0
  non-expired, pending or exhausted records are deleted.
- **SC-002**: A backlog of at least 10 times the batch size is fully purged in one run without
  error.
- **SC-003**: With two instances purging at once, the result is identical to one instance
  purging: no errors, and no record deleted more than once.
- **SC-004**: All existing payment, refund, cancellation, dead-letter and outbox tests still
  pass unchanged.

## Assumptions

- **Defaults**:

  | Setting | Default |
  |---|---|
  | Published outbox entries | 30 days |
  | Inbound dead letters | 90 days |
  | Cancellation markers | 180 days |
  | Run interval | every hour |
  | Batch size | 1 000 rows |

  These are informed defaults, and every one of them is configurable.
- **"Switched off"** means a retention value of 0. The store is then kept forever, which is
  today's behaviour.
- **Constitution**: covered by v1.4.0, Principle VI "Data retention". It replaced the v1.3.0
  rules that published outbox entries are always kept and markers are never removed.
- **Out of scope**:
  - archiving to another store before deletion;
  - purging payments or other business data;
  - any HTTP API or manual purge command (operators can still run SQL);
  - per-row status for dead letters (for example "replayed"). Purging is by age only.
