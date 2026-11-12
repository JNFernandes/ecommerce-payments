# Research: Dead-Letter Retry (US-03)

Phase 0 output for [plan.md](plan.md). Each item: Decision / Rationale / Alternatives considered.

## R1. Inbound dead letters — does the Consumer store failed inbound messages?

- **Decision**: No. **Inbound retry is out of scope** for US-03 (spec FR-016).
- **Rationale**: `OrderPlacedConsumer` has no inbound dead-letter structure. Malformed envelopes
  are logged and committed (poison messages, never retried by design); handler failures leave
  the offset uncommitted so Kafka redelivers. The only dead-letter store is
  `payment_dead_letters`, written by `PaymentEventPublisher` for **outbound** publish failures.
- **Alternatives considered**: adding an inbound dead-letter table now — rejected, the request
  makes it conditional on one already existing.

## R2. Where the dead-letter persistence lives ("extend the existing repository")

- **Decision**: Move the existing inline dead-letter write out of
  `PaymentEventPublisher.WriteDeadLetterAsync` into a new `PaymentDeadLetterRepository`
  (`Infrastructure/Persistence`, interface `IPaymentDeadLetterRepository` in the same project),
  and extend it with claim/resolve/record-failure operations. The publisher and the retry
  worker both use it.
- **Rationale**: There is no dead-letter repository today — the publisher writes through
  `PaymentsDbContext` directly. Extracting that one existing write (rather than writing a second
  path next to it) keeps a single owner of `payment_dead_letters`, honours "extend, don't
  duplicate", and puts persistence back in the Repository layer (Principle I).
- **Alternatives considered**: add the methods to `IPaymentRepository` — rejected, that
  interface lives in the Service layer and models the `Payment` aggregate, which the worker
  never touches; dead-letter rows are infrastructure bookkeeping, not a domain aggregate.

## R3. Raw publish method shared by both paths

- **Decision**: Add `PublishRawAsync(string topic, string key, string payload, CancellationToken)`
  to `PaymentEventPublisher`, exposed through a new Infrastructure interface
  `IRawEventPublisher`. It **throws** on failure and never dead-letters. `PublishAsync` /
  `PublishFailedAsync` serialize as today, then call `PublishRawAsync` inside their existing
  try/catch, which creates the dead-letter row (now with topic and key). The retry worker calls
  `PublishRawAsync` directly and updates the existing row on failure.
- **Rationale**: One producer, one publish code path; the dead-letter fallback stays exclusive
  to the normal path (FR-006). The interface makes the retry processor unit-testable with Moq.
  `IPaymentEventPublisher` (Service layer) is unchanged, so `ProcessPaymentHandler` and its
  tests are untouched.
- **Alternatives considered**: a second producer for retries — rejected (parallel component,
  extra broker connection); a `bool deadLetterOnFailure` flag on the existing methods —
  rejected, they re-serialize from domain events and would generate a new `eventId`, breaking
  FR-005.
- **DI**: register `PaymentEventPublisher` once as a singleton and forward both
  `IPaymentEventPublisher` and `IRawEventPublisher` to that instance.

## R4. Claiming: transaction scope and SKIP LOCKED

- **Decision**: **One short transaction per row.** For up to `BatchSize` iterations per cycle:
  begin transaction → claim one due row with raw SQL
  `SELECT * FROM payment_dead_letters WHERE "Status" = 'PendingRetry' AND "NextRetryAt" <= @now
  ORDER BY "NextRetryAt" LIMIT @limit FOR UPDATE SKIP LOCKED` (via `FromSql`, limit 1) →
  publish → mark Resolved or record the failure → `SaveChanges` → commit. Stop the cycle early
  when no due row is returned.
- **Rationale**: Row locks are held only for one publish (bounded by the producer's
  `MessageTimeoutMs` = 10 s), each row's outcome is committed independently (a crash mid-cycle
  loses at most the row in flight, which is safely retried), and `SKIP LOCKED` makes concurrent
  instances pick disjoint rows without waiting (FR-010). Same pattern as `ecommerce-orders`
  ADR 0005. The claim method takes `limit` as a parameter so the integration test can claim
  batches from two concurrent transactions and assert they are disjoint.
- **Alternatives considered**: one transaction for the whole batch — rejected, it holds up to
  20 row locks for up to 20 × 10 s and a crash rolls back already-published rows' bookkeeping;
  lease column (`ClaimedUntil`) committed before publishing — rejected, extra state and
  lease-expiry logic that SKIP LOCKED already provides.

## R5. Backoff formula and the Exhausted decision

- **Decision**: After a failed attempt, `RetryCount' = RetryCount + 1`. If
  `RetryCount' >= MaxRetries` → `Exhausted` (no `NextRetryAt` change, error logged). Otherwise
  `NextRetryAt = now + min(BaseDelayMs × 2^(RetryCount' − 1), MaxDelayMs)`. Computation uses
  `double`/`TimeSpan` with a cap before multiplication to avoid overflow for large counts.
  Implemented as a pure static `DeadLetterRetryPolicy` (no I/O) for direct unit testing.
- **Rationale**: Matches spec FR-008/FR-009 and the Assumptions (worker attempts only).
  Defaults give 5, 10, 20, 40, 80, 160, 320 s between the 8 attempts.
- **Alternatives considered**: jitter — deferred; with per-row SKIP LOCKED there is no
  thundering-herd risk worth the test complexity.

## R6. Time source

- **Decision**: Inject `TimeProvider` (registered as `TimeProvider.System`) into the retry
  processor, the worker (`PeriodicTimer` via `TimeProvider.CreateTimer`-backed overload
  `new PeriodicTimer(interval, timeProvider)`), the repository claim (`now` is passed in, not
  read from the DB clock) and `PaymentEventPublisher` (dead-letter `CreatedAt`/`NextRetryAt`).
  Tests use `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing` 10.11.0.
- **Rationale**: FR-011; the due comparison uses the application clock, so a fake clock fully
  controls which rows are due. SQL `now()` is never used for decisions.
- **Alternatives considered**: `DateTimeOffset.UtcNow` / DB `now()` — not controllable in tests.

## R7. Startup and shutdown behaviour

- **Decision**: `DeadLetterRetryWorker.ExecuteAsync` starts with `await Task.Yield()`, then runs
  a cycle and waits on `PeriodicTimer.WaitForNextTickAsync(stoppingToken)`. Each cycle runs in
  its own DI scope inside a catch-all that logs and continues (`OperationCanceledException` on
  shutdown exits the loop). The stopping token is passed to every DB and publish call.
- **Rationale**: Nothing in the worker connects to Kafka before the first await, so startup is
  never blocked (FR-012, Constitution VI "Startup & broker availability"). .NET 10 already runs
  `ExecuteAsync` on a background task, but the explicit yield keeps the guarantee independent of
  host version. Producer construction does not contact the broker.
- **Note**: `OrderPlacedConsumer` calls the blocking `Consume` in `ExecuteAsync`; under .NET 10
  this no longer blocks host startup, so registering the worker after it is safe. Changing the
  consumer is out of scope.

## R8. Destination topic and message key for existing and new rows

- **Decision**: Add `Topic` (text, not null) and `MessageKey` (text, not null). The migration
  backfills existing rows: `MessageKey = "PaymentId"::text`,
  `Topic = CASE "EventType" WHEN 'PaymentProcessedEvent' THEN 'payments.payment-processed'
  WHEN 'PaymentFailedEvent' THEN 'payments.payment-failed' ELSE '' END`. New rows get the topic
  and key actually used by the failed publish.
- **Rationale**: The publisher always used `PaymentId.ToString()` as key and these two topic
  names (the configured defaults). An empty topic for an unknown type makes the publish fail,
  so the row follows the normal backoff path and ends `Exhausted` (spec edge case) — never
  silently dropped.

## R9. Status storage and indexing

- **Decision**: `DeadLetterStatus` enum (`PendingRetry`, `Resolved`, `Exhausted`) stored as
  string (`HasConversion<string>()`, max length 20), consistent with `payments.Status`. Partial
  index `IX_payment_dead_letters_due` on `("NextRetryAt") WHERE "Status" = 'PendingRetry'`.
- **Rationale**: Readable in SQL (docs example INSERT), matches existing conventions; the partial
  index keeps the claim query cheap as resolved rows accumulate.

## R10. Test placement

- **Decision**: Add a new unit-test project `tests/Ecommerce.Payments.Infrastructure.Tests`
  (xUnit, Moq, FakeTimeProvider) for `DeadLetterRetryPolicy` and `DeadLetterRetryProcessor`.
  Claim-query tests go in `Ecommerce.Payments.Integration.Tests` (PostgreSQL container); the
  end-to-end retry test goes in `Ecommerce.Payments.Component.Tests` (Kafka + PostgreSQL).
- **Rationale**: Existing unit-test projects reference only Domain/Service; the retry logic lives
  in Infrastructure as requested. Keeping it as pure unit tests (no containers) follows
  Principle V. Integration/Component projects already reference Infrastructure.
- **Alternatives considered**: putting unit tests in the Integration project — rejected, it mixes
  test layers.

## R11. Options validation

- **Decision**: `DeadLetterRetryOptions` (section `DeadLetterRetry`) bound with
  `AddOptions<>().Bind(...).Validate(...).ValidateOnStart()`: all values > 0 and
  `MaxDelayMs >= BaseDelayMs`.
- **Rationale**: Misconfiguration fails fast at startup without touching Kafka; no new package
  needed (lambda validation instead of DataAnnotations).
