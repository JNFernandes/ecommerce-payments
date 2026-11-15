# 0004. Per-order serialisation of placement and cancellation

- **Status**: Accepted
- **Date**: 2026-11-15

## Context

`orders.order-placed` and `orders.order-cancelled` are different topics with no ordering between
them, so Payments can receive an order's cancellation **before** its placement. This happens, for
example, when Inventory rejects the stock and Orders cancels the order quickly. Until US-04,
Payments ignored such a cancellation ("no payment exists") and then charged the order when the
placement arrived. Nobody was left to refund that charge.

Recording the early cancellation (a *cancellation marker*) and checking for it on placement fixes
the sequential case. It does not fix the concurrent case: the two messages are handled by
different consumers and can be processed at the same moment. Under READ COMMITTED, this sequence
is possible:

1. The placement checks for a marker and finds none.
2. The cancellation checks for a payment, finds none, and writes a marker.
3. The placement saves a `Processed` payment and commits.

Both commit, leaving a charged order that is cancelled and never refunded. The marker and the
payment live in different tables, so no unique constraint can prevent this.
`PlaceCancelRaceTests.Without_Order_Lock_The_Race_Leaves_Processed_Payment_With_Marker`
reproduces it.

## Decision

We will:

1. **Record cancellation markers.** When an `OrderCancelled` finds no payment, Payments stores a
   marker in `order_cancellation_markers`, with primary key `OrderId`, so there is at most one
   marker per order. A later `OrderPlaced` for that order creates no payment and publishes
   nothing (Constitution v1.2.0, Principle VI "Cancellation markers").
2. **Take a per-order lock.** Both handlers run their read-decide-write step in one transaction,
   opened through `IOrderUnitOfWork.BeginAsync(orderId)`. Its first statement takes a
   transaction-scoped PostgreSQL advisory lock, `pg_advisory_xact_lock(key)`. The key is the
   order id folded to 64 bits by XOR-ing its two halves. A second transaction on the same order
   waits until the first commits, then sees its marker or payment. Whichever commits first wins.
3. **Bound the wait.** A transaction-local `lock_timeout` (`Persistence:OrderLockTimeoutMs`,
   default 5000) bounds the wait. A timeout is treated as a transient database failure: the
   handler's `PersistenceRetry` re-runs the whole unit, then the consumer retries the message in
   place.
4. **Publish only after commit.** Kafka is never called while the lock is held.

This is the same decision as `ecommerce-inventory` ADR 0005 for the same race.

Rejected alternatives:

- **SERIALIZABLE isolation**: every conflict becomes a serialization failure to retry, including
  false positives.
- **Re-checking for a marker after committing the payment**: a charge would briefly exist and
  need a compensating refund.
- **`SELECT … FOR UPDATE` on a row**: in the racy case neither row exists yet, so there is
  nothing to lock.
- **A shared per-order registry table used only as a lock**: the same idea as an advisory lock,
  with more schema.

## Consequences

- A cancelled order is never charged without a refund, whatever the timing.
  `PlaceCancelRaceTests` proves this on a real database with 50 concurrent rounds and both
  forced orders.
- All work on one order is serialised, while different orders still run in parallel. An
  advisory-key collision only serialises two unrelated orders briefly.
- Every new handler that reads or changes the state of an order **must** run inside
  `IOrderUnitOfWork` and publish only after commit.
- Markers are kept indefinitely: one small row per early cancellation. Expiring them is out of
  scope.
- Early cancellations ignored before this change are not backfilled.
