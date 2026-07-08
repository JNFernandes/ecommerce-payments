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
    was fully processed (DB save + publish, or DB save + dead-letter on publish failure). Never
    auto-commit before processing completes.
  - MUST catch all exceptions from the Service layer so a single bad message never crashes the
    host process; unhandled exceptions are logged and routed to retry/dead-letter, not thrown.

- **Service** (`Application/` — Application/Handler layer)
  - Orchestrates the end-to-end workflow for one consumed event: calls Domain, calls Repository,
    calls Publisher, in that exact order (see Principle II).
  - Coordinates transactions/unit-of-work boundaries.
  - MUST NOT contain business rules — those belong exclusively to Domain. A Service method that
    branches on payment amounts, statuses, or business conditions is a violation.
  - MUST NOT call the Kafka producer directly before the Repository has confirmed a successful
    save.

- **Domain** (`Domain/`)
  - Sole home of all business logic. `Payment` is the aggregate root.
  - `PaymentStatus` enum: `PENDING`, `PROCESSED`, `FAILED`. Bare strings for status are
    prohibited.
  - Domain methods encapsulate all state transitions and invariants, e.g. `Payment.Process()`,
    `Payment.Fail(reason)`. External code MUST NOT mutate `Payment` state directly (no public
    setters on status).
  - Raises domain events (`PaymentProcessed`, `PaymentFailed`) as part of executing a state
    transition. Domain events are in-process objects at this stage — they are not yet Kafka
    messages.
  - MUST be plain C# — no dependency on EF Core, Confluent.Kafka, or `.NET Worker` types. Domain
    tests MUST be able to run with zero infrastructure.

- **Repository** (`Infrastructure/Persistence/`)
  - Abstracts EF Core / PostgreSQL persistence behind an interface (e.g. `IPaymentRepository`)
    that the Service layer depends on.
  - MUST NOT leak EF Core entities/`DbContext` types into the Domain layer — map explicitly.
  - Persists the `Payment` aggregate and MUST be the **first and only** durable write in the
    flow before any Kafka publish is attempted (see Principle II).

- **Publisher** (`Infrastructure/Messaging/`)
  - Wraps the Confluent.Kafka producer behind an interface (e.g. `IPaymentEventPublisher`).
  - Serializes domain events into Kafka integration event payloads and publishes them.
  - Is invoked by the Service layer **only after** the Repository call has returned success.
  - MUST NOT be called from Domain or Repository layers directly — publishing is always
    initiated by the Service layer, never as a side effect of persistence.

### II. Event-Driven Write Flow Integrity (NON-NEGOTIABLE)

The write flow MUST follow this exact sequence and MUST NOT deviate:

1. **Consumer** receives an `OrderPlaced` message from `orders.order-placed` and deserializes it
   into a strongly-typed integration event DTO.
2. **Consumer** hands the DTO to the **Service** layer.
3. **Service** invokes **Domain**: `Payment.Process()` (or the failure path, `Payment.Fail(reason)`)
   executes business logic and raises the corresponding domain event
   (`PaymentProcessed` / `PaymentFailed`).
4. **Service** calls **Repository** to persist the `Payment` aggregate to **PostgreSQL first**.
5. **Only after** a successful database save does **Service** call **Publisher** to publish the
   corresponding event (`PaymentProcessed` → `payments.payment-processed`,
   `PaymentFailed` → `payments.payment-failed`) to Kafka.
6. **Consumer** commits the Kafka offset only after step 5 (or after step 4 + dead-letter
   handling, see below) completes.

**Failure handling:**
- If the DB save (step 4) fails: **no Kafka publish MUST occur.** Log the error with full
  context and retry the DB save (with backoff) before falling back to dead-letter. Do not
  commit the consumer offset — the message MUST be reprocessed.
- If the Kafka publish (step 5) fails **after** a successful DB save: log the error with full
  context, store the pending event in a PostgreSQL dead-letter table for retry, and still
  commit the consumer offset (the DB state is already correct and durable — do not reprocess
  the whole `OrderPlaced` message just to re-attempt a publish).
- Publishing to Kafka before a confirmed database save is a **critical violation** under any
  circumstance.

**Idempotency (NON-NEGOTIABLE):**
- Kafka delivery is at-least-once. The Consumer → Service path MUST be idempotent: processing
  the same `OrderPlaced` message (same `eventId`/`aggregateId`) twice MUST NOT create a
  duplicate `Payment` or publish a duplicate event. Use the inbound event's `eventId` (or the
  order's aggregate id) as a deduplication key checked by the Service/Repository before domain
  execution.

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
- Consumes `OrderPlaced` from topic `orders.order-placed`.
- Consumer group and offset commit strategy MUST guarantee at-least-once delivery combined with
  the idempotency guarantee in Principle II.
- Failed message processing MUST be retried with exponential backoff before being routed to a
  dead-letter table/topic.

**Publishing rules:**
- Publishes `PaymentProcessed` to `payments.payment-processed`.
- Publishes `PaymentFailed` to `payments.payment-failed`.
- Each domain event maps to exactly one Kafka topic.
- Topics follow the naming convention: `payments.<event-name>` in kebab-case.
- Events MUST only be published after a confirmed PostgreSQL save (Principle II).

**Event payload rules:**
- Every event MUST include: `eventId` (UUID), `occurredAt` (ISO timestamp), `aggregateId`
  (payment/order UUID), and `version` (integer).
- Payloads MUST be serialized as JSON.
- Breaking changes to a payload schema require a new topic version (e.g.
  `payments.payment-processed.v2`).
- Event classes MUST be documented with XML doc comments describing every field.

**Failure handling:**
- If Kafka publish fails after a successful DB save, the error MUST be logged with full context.
- Failed events MUST be stored in a dead-letter table in PostgreSQL for retry.
- A Kafka publish failure MUST NOT cause the already-persisted `Payment` state to be rolled back
  — the DB save already succeeded and is the source of truth.

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

Introducing a new runtime dependency that duplicates a capability already provided by the stack
above requires an ADR in `docs/adr/` before the PR can be merged.

## Documentation Standards

All public classes and methods MUST carry XML doc comments describing what the class/method
does, its inputs, and its outputs.
Kafka event payloads (`OrderPlaced` consumed; `PaymentProcessed`, `PaymentFailed` published)
MUST be documented inline in the event class XML doc comments, including field names, types,
and semantics.
Each project/module MUST include a `README.md` describing its responsibility, the topics it
consumes/produces, and its position in the Consumer → Service → Domain → Repository → Publisher
flow.
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

**Version**: 1.0.0 | **Ratified**: 2026-07-08 | **Last Amended**: 2026-07-08