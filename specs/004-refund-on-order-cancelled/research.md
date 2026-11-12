# Research: Refund on Order Cancelled (US-04)

Phase 0 output for [plan.md](plan.md). Format: Decision / Rationale / Alternatives considered.

## R1. What "reuse" means — inventory of US-01..US-03

| Requested reuse | What actually exists | Decision |
|---|---|---|
| Consumer base | None. `OrderPlacedConsumer` holds its own loop, commit and catch-all logic. | **Extract** `KafkaConsumerBase` (user decision, R2). |
| Idempotency (processed events) | None. `ProcessPaymentHandler` deduplicates by state (`ExistsByOrderIdAsync`). | **State-based** idempotency (user decision, R3). No inbox table. |
| Dead-letter + retry worker | `PaymentEventPublisher` dead-letters on publish failure; US-03 worker republishes any row by its stored topic/key. | **Reuse as is.** A new `PublishRefundedAsync` goes through the same `PublishOrDeadLetterAsync` path; the worker needs no change. |

## R2. Consumer base class

- **Decision**: Extract `KafkaConsumerBase` (abstract `BackgroundService`, Consumer project,
  `Consumers/`) holding: consumer construction from `KafkaOptions` (manual commit, earliest
  offset), `ExecuteAsync` loop (`Task.Yield()` first, subscribe to `Topic`, consume, catch
  `ConsumeException`, call `HandleMessageAsync`, commit when it returns `true`, catch-all log
  without commit), and `Dispose`. Subclasses provide `Topic`, a log name, and
  `internal abstract Task<bool> HandleMessageAsync(string rawMessage, CancellationToken)`.
  `OrderPlacedConsumer` keeps its constructor and `HandleMessageAsync` signature, so its
  existing tests are the regression guard.
- **Rationale**: user decision; removes duplication; the added `Task.Yield()` enforces
  Constitution VI "startup must not depend on Kafka" for both consumers regardless of host
  version.
- **Behaviour changed deliberately (analysis finding, Constitution VI at-least-once)**: today
  the loop moves on after a failure and a later commit skips the failed offset. The base now
  calls `consumer.Seek(failedResult.TopicPartitionOffset)` and waits with capped exponential
  backoff (`Kafka:ConsumerRetryBaseDelayMs` 1000, `Kafka:ConsumerRetryMaxDelayMs` 60000), so
  the same message is consumed again and nothing after it is committed first. Backoff resets
  after a success.

## R3. Idempotency

- **Decision**: No inbox table (user decision). Two checks, in order:
  1. **`eventId` dedupe (Constitution II)** — after loading the payment and *before* calling
     Domain, the handler compares the inbound `eventId` with `payment.RefundSourceEventId`;
     equal → duplicate delivery, Information log, return.
  2. **State** — Domain decides: `Processed` → refund; `Refunded`/`Failed`/`Pending` → ignored.
  The cancelling event's `eventId` is stored on the payment as `RefundSourceEventId` (FR-012).
- **Rationale**: satisfies Constitution II literally (dedupe by `eventId` before domain
  execution) without a new table; the state check additionally covers Orders re-publishing a
  cancellation with a new `eventId` (orders generates a fresh `eventId` per publish attempt —
  `ecommerce-orders/src/domain/events/order-cancelled.event.ts`). Ignored cancellations record
  nothing; reprocessing them is a no-op, so no dedupe record is needed.
- **Alternatives**: `processed_events` inbox table — rejected by the user for this story.

## R4. Concurrent cancellations for the same order

- **Decision**: Persist the refund with a **conditional update**:
  `ExecuteUpdateAsync` on `payments WHERE Id = @id AND Status = 'Processed'` setting
  `Status = 'Refunded'`, `RefundedAt`, `RefundSourceEventId`. The repository returns whether a
  row was updated; `false` means another delivery refunded it first → handler logs "already
  refunded" and publishes nothing.
- **Rationale**: two concurrent consumers (rebalance overlap, two instances) can both load a
  `Processed` payment; only one conditional update can win, so at most one announcement
  (spec edge case, SC-001). No concurrency-token change to the existing insert path.
- **Alternatives**: EF concurrency token on `Status` — affects the existing mapping and needs
  exception handling; `SELECT … FOR UPDATE` — holds a lock across the domain call for no gain.

## R5. Domain design

- **Decision**: `Payment.RequestRefund(Guid sourceEventId, DateTimeOffset refundedAt)` returns a
  `RefundDecision`: either `RefundDecision.Refunded(PaymentRefunded @event)` or
  `RefundDecision.Ignored(RefundIgnoredReason reason)` (`NotCompleted` for Failed/Pending,
  `AlreadyRefunded`). Internally it calls `Refund(...)`, the strict transition that throws
  `InvalidPaymentTransitionException` unless `Processed` (Principle III). Plus
  `Payment.Rehydrate(...)` (persistence-only factory) so the repository can load an aggregate.
  `PaymentStatus.Refunded` added; the stale "reserved for a future story" doc on `Failed` fixed.
- **Rationale**: the refund rule lives only in Domain (Principle I); the Service switches on the
  decision *type* exactly as `ProcessPaymentHandler` switches on the domain-event type, and never
  inspects `Status` itself. Ignoring is a normal outcome, not an exception (Principle VI "never
  throws").
- **Time**: `refundedAt` is passed in by the handler from `TimeProvider` (testable); existing
  methods keep `DateTimeOffset.UtcNow` (unchanged, FR-013).

## R6. Inbound contract

- **Decision**: `OrderCancelledEvent` (Service/IntegrationEvents) with `TryParse` mirroring
  `OrderPlacedEvent`: required `eventId`, `occurredAt`, `aggregateId`, `version > 0`; optional
  `customerId`, `reason`. Unknown fields ignored.
- **Rationale**: matches `ecommerce-orders` `OrderCancelled` (eventId, occurredAt, aggregateId,
  version, customerId, optional reason). `customerId` is not needed for the refund, so it is not
  required — a missing customer id should not block a refund.

## R7. Outbound contract and publishing

- **Decision**: `PaymentRefundedEvent` DTO (eventId, occurredAt = refundedAt, aggregateId =
  payment id, version 1, orderId, amount, currency, refundedAt); `IPaymentEventPublisher`
  gains `PublishRefundedAsync(PaymentRefunded, ct)`, implemented through the existing
  `PublishOrDeadLetterAsync` (key = payment id, dead-letter on failure with
  `EventType = PaymentRefundedEvent`, `Topic = payments.payment-refunded`).
  `KafkaOptions` gains `OrderCancelledTopic` and `PaymentRefundedTopic`.
- **Rationale**: FR-007/FR-008; the US-03 worker already republishes by stored topic/key, so no
  change there. The US-03 migration backfill CASE only applied to pre-existing rows.

## R8. Retry of transient DB failures

- **Decision**: Move the Polly pipeline from `ProcessPaymentHandler` into a shared internal
  `PersistenceRetry.Pipeline` (Service, same settings: 3 retries, 200 ms exponential) and use it
  in both handlers for the load and the conditional update.
- **Rationale**: FR-010 "retry with exponential backoff, then the same handling"; one definition
  instead of two. Behaviour of `ProcessPaymentHandler` unchanged.

## R9. Known gaps (documented, not fixed here)

- ~~Offset commit after a persistent failure~~ — **fixed in this story** (R2, seek-back retry).
- **No inbound dead-letter**: a permanently failing inbound message retries in place forever
  (blocking its partition) instead of being routed to a dead-letter store (Constitution VI wording).
  Needs a follow-up decision; US-03 scoped inbound dead-letters out.
- **Crash between save and publish**: the refund is saved, the process dies before
  publishing/dead-lettering; the redelivery finds `Refunded` and publishes nothing → announcement
  lost. Same gap as the existing OrderPlaced flow (no transactional outbox). Follow-up candidate.
- **Cancellation before payment exists**: ignored per spec (Assumptions).

## R10. Consumer group

- **Decision**: `OrderCancelledConsumer` uses the same `Kafka:ConsumerGroupId` as
  `OrderPlacedConsumer`, subscribing to a different topic.
- **Rationale**: one service, one group; Kafka assigns partitions per subscribed topic. A
  separate group id adds config with no benefit.
