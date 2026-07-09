# Research: Process Payment from Order Placed Event

All Technical Context items were resolvable directly from the project constitution (which
pins the stack and the write-flow rules) and the feature spec. No `NEEDS CLARIFICATION` markers
remain. This document records the technical decisions made while turning those fixed constraints
into a concrete design, plus the alternatives considered for the parts the constitution leaves
open.

## 1. Idempotency enforcement

**Decision**: Enforce idempotency at the data layer with a unique constraint on `Payment.OrderId`
(the order's `aggregateId` from `OrderPlaced`), and have the Service layer perform a pre-check
(lookup by `OrderId`) before invoking `Payment.Process()`, short-circuiting with a no-op if a
payment already exists for that order.

**Rationale**: The constitution (Principle II) explicitly asks for a dedup key checked "by the
Service/Repository before domain execution," and a unique constraint gives a hard guarantee even
under concurrent redelivery (e.g., two consumer instances), whereas an application-level check
alone is racy. The pre-check in Service avoids the cost of running Domain logic unnecessarily on
a known duplicate, while the DB constraint is the actual safety net.

**Alternatives considered**:
- *Dedup solely on `eventId`*: Rejected as the sole key — the same order could theoretically be
  re-published under a new `eventId` by an upstream bug/replay tool, and the business invariant
  we actually care about is "one payment per order," not "one payment per message." `OrderId` is
  used as the constraint; `eventId` is still stored for traceability/log correlation.
- *Idempotency table keyed by `eventId` only (generic outbox-style dedup table)*: Rejected for
  this story as unnecessary extra infrastructure — the `Payment` aggregate itself already carries
  the natural key (`OrderId`) needed to detect a repeat.

## 2. Ordering guarantee between DB save and Kafka publish

**Decision**: No distributed transaction or transactional outbox pattern. Follow the
constitution literally: Repository save commits first (single PostgreSQL transaction covering
just the `Payment` row); only on confirmed success does the Service layer call the Publisher.
On publish failure post-save, write a row to a `PaymentDeadLetter` table in the same database and
still commit the Kafka offset.

**Rationale**: Principle II spells out this exact sequence and failure handling, including that a
publish failure after a successful save must NOT roll back the payment and must NOT reprocess the
original message. A full transactional outbox (writing the outgoing event in the same DB
transaction as the aggregate, with a separate relay process publishing it) would give stronger
delivery guarantees, but the constitution already defines a simpler, explicit dead-letter-based
alternative and reprocessing story (US-02) — introducing a second, competing consistency
mechanism would violate Principle I's single-responsibility intent and add scope this story does
not need.

**Alternatives considered**:
- *Transactional outbox + relay*: Rejected as over-engineering relative to the constitution's
  explicit, simpler contract for this story; would also introduce a new "relay" component with
  its own layer ambiguity.
- *Two-phase commit across PostgreSQL and Kafka*: Rejected — not supported by Kafka, and
  explicitly the kind of complexity the constitution's simpler save-then-publish contract avoids.

## 3. Retry/backoff for transient DB save failures

**Decision**: Use a bounded exponential backoff retry (e.g., via `Polly`) inside the Repository
call from the Service layer for transient PostgreSQL errors (connection failures, timeouts).
Exhausting retries surfaces the failure to the Consumer, which does not commit the offset,
letting Kafka's own redelivery drive the next attempt.

**Rationale**: The spec (User Story 3 / FR-007) requires retry rather than data loss, but does
not mandate a specific retry count or interval — this is correctly a planning-level detail per
the spec's Assumptions section. Combining a short in-process retry (for genuinely transient
blips) with Kafka-level redelivery (for longer outages) avoids blocking the consumer thread for
too long while still recovering automatically once PostgreSQL is healthy again.

**Alternatives considered**:
- *No in-process retry, rely solely on Kafka redelivery*: Simpler, but means every transient
  hiccup pays the cost of a full redelivery cycle; rejected in favor of a small bounded retry
  first.
- *Unbounded retry loop inside the Service call*: Rejected — would block the consumer
  indefinitely on a sustained outage instead of surfacing back to the Consumer's
  retry/dead-letter flow.

## 4. Kafka consumer configuration

**Decision**: Manual offset commit (`EnableAutoCommit = false`), one partition-ordered consumer
group per service instance, commit only after the Service layer reports full success (published)
or confirmed dead-letter handoff (post-save publish failure).

**Rationale**: Directly required by Principle I ("commits only after the Service layer confirms
the message was fully processed") and Principle VI (at-least-once + idempotency).

**Alternatives considered**:
- *Auto-commit*: Rejected outright — constitutionally prohibited or reprocessing.

## 5. Testing infrastructure

**Decision**: `Testcontainers.PostgreSql` for integration tests (Consumer→Repository, real DB);
`Testcontainers.Kafka` + `Testcontainers.PostgreSql` together for component tests (full
consume → save → publish flow). Unit tests for Domain and Service layers use no containers.

**Rationale**: Matches Principle V's three-tier testing strategy exactly; `testcontainers-dotnet`
is already the constitution's pinned tool.

**Alternatives considered**:
- *In-memory EF Core provider for integration tests*: Rejected — Principle V explicitly requires
  a real containerized PostgreSQL for integration tests, since in-memory providers don't
  validate real constraints (e.g., the unique `OrderId` index this story relies on for
  idempotency).

## 6. Event payload versioning

**Decision**: `version` field (int) starts at `1` for both `OrderPlaced` (as consumed/assumed)
and `PaymentProcessed` (as produced). No schema registry introduced for this story.

**Rationale**: Constitution Principle VI requires the `version` field but does not mandate a
schema registry; introducing one is a cross-cutting infrastructure decision better suited to an
ADR than to a single user story.

**Alternatives considered**:
- *Confluent Schema Registry with Avro/Protobuf*: Rejected for this story — worthwhile future
  hardening, but out of scope; would require an ADR per the constitution's Technology Stack
  section ("Introducing a new runtime dependency... requires an ADR").
