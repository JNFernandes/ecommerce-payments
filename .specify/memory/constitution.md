<!--
Sync Impact Report
==================
Version change: 1.3.0 → 1.4.0 (MINOR — new subsection and materially expanded guidance; no
principle removed or redefined). Two "keep forever" statements are narrowed to "keep for at least
the configured retention period".

Modified principles:
- VI. Kafka Consumption & Event Publishing:
  - "Cancellation markers": markers are no longer "never expired or removed". They are kept for
    at least the marker retention period, then may be purged.
  - "Outbox relay": published entries are kept for support until purged under "Data
    retention"; the separate-concern note is removed.

Added sections:
- VI → "Data retention" subsection. It covers:
  - a configurable retention period for each of published outbox entries, inbound dead
    letters and cancellation markers, where 0 keeps the data forever;
  - `Pending` and `Exhausted` outbox entries are never purged;
  - purging is done by age, in bounded batches, safe across instances, and never blocks
    startup or payment processing;
  - each run logs the counts it deleted;
  - the marker retention must exceed the longest expected `OrderPlaced` delay.

Removed sections: none.

Templates / runtime docs:
- ✅ .specify/templates/plan-template.md — the Constitution Check is derived at plan time; no
  change.
- ✅ .specify/templates/spec-template.md — no change.
- ✅ .specify/templates/tasks-template.md — no change.
- ✅ .specify/templates/commands/*.md — the directory is not present in this repo.
- ✅ CLAUDE.md — version reference updated to v1.4.0, with a retention note.
- ⚠ README.md, docs/outbox.md, docs/consumed-dead-letters.md — they still say entries, dead
  letters and markers are never removed (true on `main` until US-09 ships). Update them in
  US-09 (specs/009-data-retention).
- ⚠ specs/001–008 — written against earlier versions; intentionally not modified.

Follow-up TODOs:
- The code does not purge anything yet. Tracked by US-09.
-->

# ecommerce-payments Constitution

## Core Principles

### I. Domain-Driven Design & Layered Architecture (NON-NEGOTIABLE)

This is a **pure event-driven Background Worker Service** — no REST API, no GraphQL, no
inbound HTTP surface. The codebase MUST follow a strict five-layer model:

**Consumer → Service → Domain → Repository → Publisher**

Each layer has a single responsibility. Skipping a layer, merging two layers' concerns into
one class, or calling a layer out of order is a constitutional violation.

- **Consumer** (`Consumer/` or `Workers/`)
  - Kafka `IConsumer<TKey, TValue>` background hosts (`BackgroundService` / `IHostedService`).
  - Subscribes to topics, deserializes the raw Kafka message into an **Integration Event DTO**
    (a strongly-typed class, never `dynamic`/`object`).
  - Validates the envelope shape only (e.g. required fields present, correct schema version).
  - Delegates immediately to the Service layer. MUST NOT contain business logic.
  - Owns offset commit strategy: commits **only after** the Service layer confirms the message
    was fully processed. That means the transaction holding the state change and its outbox
    entry has committed, or the decision to ignore has been made. It never waits for the Kafka
    publish, which the outbox relay performs, and never auto-commits before processing
    completes.
  - MUST catch all exceptions from the Service layer so a single bad message never crashes the
    host process; unhandled exceptions are logged and routed to retry/dead-letter, not thrown.

- **Service** (`Application/` — Application/Handler layer)
  - Orchestrates the end-to-end workflow for one consumed event: calls Domain, then calls the
    Repository to save the state change **and** the resulting outbox entry in one transaction
    (see Principle II).
  - Coordinates transactions/unit-of-work boundaries.
  - MUST NOT contain business rules — those belong exclusively to Domain. A Service method that
    branches on payment amounts, statuses, or business conditions is a violation.
  - MUST NOT call the Kafka producer or the Publisher. Outgoing events are recorded in the
    outbox and published by the outbox relay (Principle VI "Outbox relay").

- **Domain** (`Domain/`)
  - Sole home of all business logic. `Payment` is the aggregate root.
  - `PaymentStatus` enum: `PENDING`, `PROCESSED`, `FAILED`, `REFUNDED`. Bare strings for status
    are prohibited.
  - Domain methods encapsulate all state transitions and invariants, e.g. `Payment.Process()`,
    `Payment.Fail(reason)`, `Payment.Refund()`. External code MUST NOT mutate `Payment` state
    directly (no public setters on status).
  - **Refund rule:** a payment MAY only be refunded from the `PROCESSED` state
    (`PROCESSED → REFUNDED`). The Domain is the only place this rule is decided; see Principle VI
    "Refund rules" for how ignorable refund requests are handled without throwing.
  - Raises domain events (`PaymentProcessed`, `PaymentFailed`, `PaymentRefunded`) as part of
    executing a state transition. Domain events are in-process objects at this stage — they are
    not yet Kafka messages.
  - MUST be plain C# — no dependency on EF Core, Confluent.Kafka, or `.NET Worker` types. Domain
    tests MUST be able to run with zero infrastructure.

- **Repository** (`Infrastructure/Persistence/`)
  - Abstracts EF Core / PostgreSQL persistence behind an interface (e.g. `IPaymentRepository`)
    that the Service layer depends on.
  - MUST NOT leak EF Core entities/`DbContext` types into the Domain layer — map explicitly.
  - Persists the `Payment` aggregate together with the outbox entry for each domain event it
    raised, in **one** atomic transaction. A state change MUST NOT be saved without its outbox
    entry, and an outbox entry MUST NOT be saved without its state change (see Principle II).

- **Publisher** (`Infrastructure/Messaging/`)
  - Wraps the Confluent.Kafka producer behind an interface (e.g. `IPaymentEventPublisher`).
  - Serializes domain events into Kafka integration event payloads. Serialization happens when
    the outbox entry is written, so the stored payload is exactly what will be published.
  - Publishes only stored outbox entries, and is invoked **only** by the outbox relay. The relay
    reads entries that are already committed, so a publish can never precede its save.
  - MUST NOT be called from Domain, Repository or Service code.

### II. Event-Driven Write Flow Integrity (NON-NEGOTIABLE)

The write flow MUST follow this exact sequence and MUST NOT deviate:

1. **Consumer** receives an `OrderPlaced` message from `orders.order-placed` and deserializes it
   into a strongly-typed integration event DTO.
2. **Consumer** hands the DTO to the **Service** layer.
3. **Service** invokes **Domain**: `Payment.Process()` (or the failure path, `Payment.Fail(reason)`)
   executes business logic and raises the corresponding domain event
   (`PaymentProcessed` / `PaymentFailed`).
4. **Service** calls **Repository** to persist, in **one PostgreSQL transaction**, the
   `Payment` aggregate and an outbox entry for the domain event. The outbox entry holds the
   serialized integration event, its topic (`PaymentProcessed` → `payments.payment-processed`,
   `PaymentFailed` → `payments.payment-failed`) and its key (the payment id).
5. **Consumer** commits the Kafka offset once that transaction has committed. It does not wait
   for the event to be published.
6. The **outbox relay**, a background service, publishes committed outbox entries to Kafka
   through the **Publisher**, unchanged, and marks each one `Published`
   (Principle VI "Outbox relay").

The same sequence applies to `OrderCancelled` from `orders.order-cancelled`:
- Domain executes `Payment.Refund()` and raises `PaymentRefunded`.
- The refund and its outbox entry for `payments.payment-refunded` are saved in one
  transaction.
- The relay publishes the entry.

An `OrderCancelled` for an order with no payment instead records a cancellation marker
(Principle VI "Cancellation markers"). That write creates no outbox entry, and the offset is
committed only after it is saved.

**Failure handling:**
- If the transaction (step 4) fails, **nothing is saved and nothing is published**, because the
  outbox entry is part of the failed transaction. Log the error with full context and retry with
  backoff. Do not commit the consumer offset — the message MUST be reprocessed.
- If publishing an outbox entry fails, the relay logs the error with full context and retries
  later (Principle VI "Outbox relay"). The state change is already correct and durable, so the
  inbound message is never reprocessed to re-attempt a publish.
- A process crash at any point loses nothing. Either the transaction never committed and the
  inbound message is redelivered, or the outbox entry exists and the relay publishes it after
  restart.
- Publishing to Kafka before a confirmed database save is a **critical violation** under any
  circumstance. Publishing anything other than a committed outbox entry is a violation too.

**Idempotency (NON-NEGOTIABLE):**
- Kafka delivery is at-least-once. The Consumer → Service path MUST be idempotent: processing
  the same `OrderPlaced` or `OrderCancelled` message twice MUST NOT create a duplicate `Payment`,
  apply a duplicate state transition, or publish a duplicate event.
- In order events the order is identified by `aggregateId` (the order id). At most one `Payment`
  exists per order: the order's `aggregateId` is the idempotency key for payment creation.
- No `Payment` is ever created for an order that has a cancellation marker, and no marker is
  created for an order that has a `Payment` (Principle VI "Cancellation markers").
- Consumers MUST deduplicate redelivered messages by the inbound event's `eventId`, checked by
  the Service/Repository before domain execution.

### III. Type Safety & Input Validation

All code MUST be strictly typed. `dynamic` and `object` are **banned** for any data that
flows through Consumer, Service, Domain, or Publisher — always use strongly typed C# classes
(records or classes) for integration events, DTOs, and payloads.

- Nullable reference types MUST be enabled project-wide (`<Nullable>enable</Nullable>`).
- Every inbound Kafka message MUST be deserialized into a dedicated integration event class,
  never a loosely-typed dictionary or `JsonElement` passed further than the deserialization
  boundary.
- The `PaymentStatus` enum MUST be used for all status fields — bare string literals are
  prohibited.
- Domain invariants MUST be enforced by throwing dedicated domain exceptions (e.g.
  `InvalidPaymentTransitionException`), never by returning `null` or a bare `bool`.
- Unhandled exceptions MUST be caught at the Consumer boundary — no silent failures, no
  swallowed exceptions without logging.

### IV. Test Coverage

All domain logic and Service-layer handlers MUST have unit tests.

- Every Domain method (`Payment.Process()`, `Payment.Fail(reason)`, etc.) and every Service
  handler MUST have at least one unit test covering the happy path and at least one covering an
  error/edge case (e.g. invalid transition, duplicate event).
- Domain tests MUST NOT depend on EF Core, PostgreSQL, or Kafka — pure unit tests only, no test
  doubles for infrastructure needed because Domain has no infrastructure dependencies.
- Service-layer tests MAY use Moq to mock `IPaymentRepository` and `IPaymentEventPublisher`.
- A PR that adds a handler or domain method without corresponding tests MUST NOT be merged.

### V. Testing Strategy

Three test layers are required:

- **Unit tests** — Domain logic and Service handlers in isolation. No PostgreSQL, no Kafka.
  Tools: xUnit, Moq.
- **Integration tests** — verifies Consumer → Repository: a consumed message results in the
  correct `Payment` state persisted to a real (containerized) PostgreSQL instance. Tools: xUnit,
  testcontainers-dotnet.
- **Component tests** — full worker host with real dependencies: consume `OrderPlaced` from a
  real (containerized) Kafka topic → `Payment` saved to PostgreSQL → `PaymentProcessed` /
  `PaymentFailed` published to the correct output topic. Only external microservices are mocked.

Minimum coverage: 80% on Domain and Application (Service) layers.
Test names MUST describe the behaviour, e.g. `Should_Publish_PaymentProcessed_After_Successful_Save`.

### VI. Kafka Consumption & Event Publishing

This service is **both a consumer and a producer** of domain/integration events.

**Consumption rules:**
- Consumes `OrderPlaced` from topic `orders.order-placed` (to create and process a payment).
- Consumes `OrderCancelled` from topic `orders.order-cancelled` (to refund a processed payment).
- In order events the order is identified by `aggregateId` (the order id).
- Consumers MUST deduplicate by `eventId` (Principle II).
- Consumer group and offset commit strategy MUST guarantee at-least-once delivery combined with
  the idempotency guarantee in Principle II.
- Failed message processing MUST be retried with exponential backoff before being routed to a
  dead-letter table/topic.

**Publishing rules:**
- Publishes `PaymentProcessed` to `payments.payment-processed`.
- Publishes `PaymentFailed` to `payments.payment-failed`.
- Publishes `PaymentRefunded` to `payments.payment-refunded`.
- Every payment event carries `aggregateId` = the **payment** id and a separate `orderId` field
  = the order id. Downstream consumers MUST use `orderId` (never `aggregateId`) to identify the
  order.
- Each domain event maps to exactly one Kafka topic.
- Topics follow the naming convention: `payments.<event-name>` in kebab-case.
- Events MUST only be published after a confirmed PostgreSQL save, and only from the outbox
  (Principle II).
- Events for the same payment MUST be published in the order their outbox entries were
  recorded. A later event MUST NOT be published while an earlier one for the same payment is
  unpublished. A failing event for one payment MUST NOT delay other payments' events.
- In healthy conditions, an outbox entry MUST be published within 2 seconds of being committed.

**Event payload rules:**
- Every event MUST include: `eventId` (UUID), `occurredAt` (ISO timestamp), `aggregateId`
  (UUID of the event's own aggregate — the payment id for events published by this service),
  and `version` (integer). Payment events additionally MUST include `orderId` (UUID).
- Payloads MUST be serialized as JSON.
- Breaking changes to a payload schema require a new topic version (e.g.
  `payments.payment-processed.v2`).
- Event classes MUST be documented with XML doc comments describing every field.

**Failure handling:**
- If publishing an outbox entry fails, the error MUST be logged with full context, and the entry
  stays in the outbox with its retry bookkeeping.
- There MUST be a single outbound store, the outbox. No separate table or mechanism for failed
  publishes is kept alongside it.
- A Kafka publish failure MUST NOT cause the already-persisted `Payment` state to be rolled back
  — the DB save already succeeded and is the source of truth.

**Refund rules:**
- A payment MAY only be refunded from the `PROCESSED` state (Principle I).
- A refund request (`OrderCancelled`) for a payment that is `FAILED` or already `REFUNDED` MUST
  be ignored: logged with full context, offset committed, no state change, no event published.
- A refund request for an order with no payment MUST record a cancellation marker (see below):
  logged, offset committed after the marker is saved, no event published.
- None of these cases may ever throw out of the Consumer/Service path.

**Cancellation markers:**
- `OrderCancelled` and `OrderPlaced` arrive on different topics with no relative ordering, so a
  cancellation can arrive before the order's payment exists. The marker records that the order
  was cancelled: order id, cancelling `eventId`, cancellation time, recorded time, optional
  reason.
- At most one marker MUST exist per order, enforced by a unique constraint in PostgreSQL.
- An `OrderPlaced` for an order that has a marker MUST NOT create, evaluate or save a `Payment`
  and MUST NOT publish any event (neither `PaymentProcessed` nor `PaymentFailed`); it is logged
  and its offset committed.
- Marker creation and payment creation for the same order MUST be mutually exclusive under
  concurrency: whichever is committed first wins and the other then observes it. The service
  MUST never end with a `PROCESSED`, unrefunded payment for a marked order, nor create a payment
  after the order's marker exists. Losing this race is a normal outcome, not an error.
- Markers deduplicate redelivered cancellations by `eventId` (Principle II). A marker MUST be
  kept for at least the marker retention period, then MAY be purged ("Data retention" below).

**Outbox relay:**
- Outbox entries MUST carry the following, in addition to the event id, event type, topic, key
  (payment id) and serialized payload:
  - `status`: `Pending`, `Published` or `Exhausted`;
  - retry count, next attempt time and last error;
  - recorded time and published time.
- A background relay MUST publish due entries (`status = Pending` and next attempt time ≤ now).
  - It polls often enough to meet the healthy-path bound above.
  - It uses exponential backoff between failed attempts, up to a configured maximum retry
    count.
  - A successful publish marks the entry `Published`; reaching the maximum marks it
    `Exhausted` and logs an error.
- Entries MUST be claimed so that several service instances never process the same entry
  (`SELECT … FOR UPDATE SKIP LOCKED` in PostgreSQL), while preserving per-payment ordering.
- The relay MUST publish the stored payload unchanged, with its stored key and its original
  `eventId`. Delivery is at-least-once: a crash after publishing but before marking an entry
  can publish it again, and downstream consumers deduplicate by `eventId`.
- Published entries are kept for support until purged under "Data retention" below.

**Data retention:**
- Three stores MAY be purged by age. Each has its own configurable retention period, and a
  period of 0 keeps that store forever:
  - `Published` outbox entries, measured from their publish time;
  - inbound dead letters, measured from when they were stored;
  - cancellation markers, measured from when they were recorded.
- `Pending` and `Exhausted` outbox entries MUST NEVER be purged, whatever their age. Purging
  `Published` entries MUST NOT change the per-payment publishing order of the remaining ones.
- Purging MUST:
  - run in the background;
  - delete in bounded batches;
  - be safe to run on several instances at once;
  - never delay startup or payment processing;
  - log how many records it deleted from each store on each run;
  - when it fails, log the error and retry on the next run.
- The marker retention period MUST be longer than the longest expected delay of an
  `OrderPlaced` behind its `OrderCancelled`. A marker purged too early would let a very late
  `OrderPlaced` charge a cancelled order.
- Payments and other business data are never purged.

**Startup & broker availability:**
- Application startup MUST NOT depend on Kafka being reachable. Kafka connectivity problems
  MUST be logged and retried in the background; they MUST NOT crash or block host startup.

### VII. Branching Strategy

This project follows a **feature-branch workflow** tied to user stories.

**Branch naming convention:**
- Feature branches: `feature/US-XX-short-description`
- Bug fixes: `fix/US-XX-short-description`
- Infrastructure: `chore/short-description`

Examples:
- `feature/US-01-consume-order-placed`
- `feature/US-02-process-payment`
- `feature/US-03-handle-payment-failure`

**Rules:**
- `main` is always stable — never commit directly to main.
- Every user story gets its own branch created from `main`.
- Branch MUST be created before any implementation begins.
- Each branch maps to exactly one user story.
- PRs MUST reference the user story: "Implements US-01: Consume OrderPlaced".
- Branches MUST be deleted after merge.
- Commit messages MUST follow Conventional Commits:
  `feat(US-01): add order placed consumer`
  `test(US-01): add unit tests for payment aggregate`
  `docs(US-01): add XML doc comments to payment events`

**Automated branch creation (MANDATORY):**
When `/speckit.specify` is invoked for a user story, Claude Code MUST:
1. Ensure the working tree is clean (no uncommitted changes).
2. Checkout main and pull latest: `git checkout main && git pull`.
3. Create and checkout the feature branch: `git checkout -b feature/US-XX-short-description`.
4. Only then proceed with generating the spec.

The branch MUST exist before any spec file is written.
Claude Code MUST NOT generate spec files while on main.

### VIII. Build & Code Quality Integrity (NON-NEGOTIABLE)

The project MUST compile and pass all quality checks at all times.

**After every task implementation, Claude Code MUST run in this order:**

1. `dotnet build` — MUST pass with zero compilation errors.
2. `dotnet format --verify-no-changes` — MUST pass; formatting drift is blocking.
3. `dotnet test` — all existing tests MUST still pass.

**Rules:**
- If any of the above fails, Claude Code MUST fix it before moving to the next task.
- A failing build, formatting violation, or failing test MUST never be committed to the branch.
- Roslyn analyzer warnings are reviewed; analyzer **errors** are blocking.
- Claude Code MUST NOT mark a task as complete until all three checks pass.
- `dynamic` and `object` usage for domain/event data is treated as a build-blocking violation,
  enforced via Roslyn analyzer rules, not just code review.

**Tooling:**
- Formatter/Linter: `dotnet format` + Roslyn analyzers
- Build: .NET 10 SDK (`dotnet build`)
- Tests: xUnit + Moq + testcontainers-dotnet

**Project settings required (minimum):**
- `Nullable` = `enable`
- `TreatWarningsAsErrors` for analyzer categories covering banned `dynamic`/`object` usage
- `ImplicitUsings` explicit and consistent across projects

## Technology Stack

| Concern | Choice |
|---------|--------|
| Runtime | .NET 10 + C# |
| Hosting model | .NET Worker Service (Background Worker, no HTTP surface) |
| Kafka client | `Confluent.Kafka` |
| ORM | Entity Framework Core |
| Persistence | PostgreSQL |
| Testing | xUnit + Moq + testcontainers-dotnet |
| Linting/Formatting | `dotnet format` + Roslyn analyzers |
| Containerization | Docker + docker-compose |
| Local infrastructure | `ecommerce-infra` (docker compose) |

Introducing a new runtime dependency that duplicates a capability already provided by the stack
above requires an ADR in `docs/adr/` before the PR can be merged.

## Documentation Standards

All public classes and methods MUST carry XML doc comments describing what the class/method
does, its inputs, and its outputs.
Kafka event payloads (`OrderPlaced`, `OrderCancelled` consumed; `PaymentProcessed`,
`PaymentFailed`, `PaymentRefunded` published) MUST be documented inline in the event class XML
doc comments, including field names, types, and semantics.
Each project/module MUST include a `README.md` describing its responsibility, the topics it
consumes/produces, and its position in the Consumer → Service → Domain → Repository → Publisher
flow.
Each new background service (hosted service / worker, e.g. a consumer or the outbox relay
worker) MUST be documented in the project root `README.md`.
Architectural decisions MUST be recorded as ADRs in `docs/adr/` using the standard template
(Context / Decision / Consequences).

## Governance

This constitution supersedes all other project conventions and style guides. When a conflict
arises, the constitution takes precedence.

**Amendment procedure**: Any amendment requires (1) a draft PR updating this file, (2) a version
bump per the policy below, (3) approval from at least one other maintainer, and (4) a migration
note if existing code must change.

**Versioning policy**:
- MAJOR: Removal or redefinition of an existing principle (breaking governance change).
- MINOR: Addition of a new principle or materially expanded guidance.
- PATCH: Clarifications, wording refinements, typo fixes, non-semantic changes.

**Compliance**: Every PR review MUST verify that no constitutional principle is violated.
Reviewers are empowered to block merges for violations without exception. New principles take
effect immediately upon merge.

**Version**: 1.4.0 | **Ratified**: 2026-07-08 | **Last Amended**: 2026-11-16