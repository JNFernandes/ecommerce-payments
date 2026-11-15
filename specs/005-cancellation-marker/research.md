# Research: Do Not Charge an Already-Cancelled Order (US-05)

All decisions below are resolved; no NEEDS CLARIFICATION remains.

## R1. How to make marker creation and payment creation mutually exclusive (FR-006)

**Decision**: Serialise all work on one order with a **transaction-scoped PostgreSQL advisory
lock** keyed by the order id (`pg_advisory_xact_lock(bigint)`), taken as the **first** statement
of the transaction in both handlers, before any read. The second transaction on the same order
waits until the first commits (or rolls back) and then sees its marker or payment.

**Rationale**: The race is "check A, check B, both see nothing, both insert". Under READ
COMMITTED, neither table's unique constraint can catch it, because the two rows live in
different tables. A lock on the order id closes the gap without changing the isolation level.
It needs no extra table and no retries on the happy path. The lock is released automatically
at commit or rollback, so a crash cannot leave it held. This is the same decision Inventory
made for the same race (`ecommerce-inventory` ADR 0005).

**Alternatives considered**:
- *SERIALIZABLE isolation*: correct, but every conflicting pair (and some false positives)
  surfaces as a serialization failure that must be retried, for every payment. That is heavier
  and harder to reason about.
- *Re-check after commit*: the payment side would commit, then look for a marker and refund. A
  Processed payment for a cancelled order would exist for a moment and need a compensating
  refund event. That breaks "never charge after a marker exists".
- *Lock a row* (`SELECT … FOR UPDATE`): in the racy case neither the payment nor the marker row
  exists yet, so there is nothing to lock.
- *One shared "order state" table with a unique key*: works, but it adds a table whose only
  purpose is to act as a lock. It is the same idea as an advisory lock, with more schema.

## R2. Advisory lock key

**Decision**: Fold the 128-bit order id to a signed 64-bit key: XOR its two 8-byte halves
(`BitConverter.ToInt64(bytes, 0) ^ BitConverter.ToInt64(bytes, 8)`). Use the single-argument
`pg_advisory_xact_lock(bigint)`.

**Rationale**: The key is deterministic, so every instance computes the same key. The database
is owned by Payments alone (ADR 0001), so no other service shares the advisory-lock key space.
A collision only serialises two unrelated orders briefly; it never causes a wrong result.

**Alternatives**: `hashtext(order_id::text)` in SQL gives only 32 bits, so collisions are more
likely. The two-int form `(classid, objid)` would need a namespace constant and still only 32
bits for the order.

## R3. Bounding the lock wait

**Decision**: Within the transaction, run `SET LOCAL lock_timeout` from a new setting,
`Persistence:OrderLockTimeoutMs` (default 5000), before taking the lock. A timeout raises a
PostgreSQL error (`55P03`). The existing `PersistenceRetry` treats it like any transient
database failure; if the error persists, the consumer retries the message in place, which is
the US-04 behaviour.

**Rationale**: A stuck transaction must not hang a consumer forever. Lock timeouts are already
handled correctly by the existing error paths.

## R4. Where the transaction boundary lives (layering)

**Decision**: Add a Service-layer port, `IOrderUnitOfWork`, with one method,
`BeginAsync(Guid orderId, CancellationToken) → IOrderTransaction`. `IOrderTransaction` exposes
`CommitAsync` and is `IAsyncDisposable`: disposing without a commit rolls back. Infrastructure
implements it as `OrderUnitOfWork` over the scoped `PaymentsDbContext`, which does:

1. `BeginTransactionAsync`
2. `SET LOCAL lock_timeout`
3. `SELECT pg_advisory_xact_lock(@key)`

On rollback it also clears the `ChangeTracker`, so a retry starts clean. The repositories share
the same scoped `DbContext`, so their reads and writes (including `ExecuteUpdateAsync` in
`SaveRefundAsync`) run inside that transaction.

**Rationale**: Constitution I says the Service "coordinates transactions/unit-of-work
boundaries" and must not touch EF Core types. A port keeps `IDbContextTransaction` and the SQL
out of the Service. The dead-letter repository's `BeginTransactionAsync` returns an EF type and
is used only within Infrastructure, so it is not reused here.

## R5. Retry scope

**Decision**: `PersistenceRetry` wraps the **whole unit**: begin, lock, reads, domain call,
write and commit. Each attempt is one delegate. Publishing happens after the delegate returns,
outside the transaction, exactly as today.

**Rationale**: After a failure inside a transaction, retrying one statement is meaningless,
because the transaction is aborted. Re-running the unit re-reads state under the lock, so a
retry after a concurrent commit sees the winner. The domain calls (`CreatePending`/`Evaluate`,
`RequestRefund`) are deterministic apart from new ids and timestamps, so re-running them is
safe.

## R6. Marker model and idempotency

**Decision**: A new plain-C# Domain type, `CancellationMarker` (in `Domain/Orders/`), with the
factory `CancellationMarker.Record(orderId, sourceEventId, cancelledAt, recordedAt, reason)`.
The factory rejects empty ids. A `Rehydrate` method is used for persistence. The marker is
never mutated. It is stored in a new table, `order_cancellation_markers`, with primary key
`OrderId`; the primary key is the unique constraint required by FR-003.

Each case is handled under the lock:

- Same `eventId` already on the marker: duplicate delivery, logged as Information, no change.
- Different `eventId`: "already marked", logged as Information, no change.

A unique violation on insert is not expected, because the lock serialises inserts. If one
happens anyway, it is a transient failure: the unit retries, sees the marker and logs it as a
duplicate. This satisfies FR-003 and FR-008.

**Rationale**: This mirrors how US-04 stores `RefundSourceEventId` on the payment, so no inbox
table is needed (the US-04 user decision). The marker has no behaviour beyond validation, but
keeping it in Domain keeps the Repository mapping explicit (Constitution I).

## R7. OrderPlaced for a marked order

**Decision**: `ProcessPaymentHandler`, under the lock:

1. Payment exists: no-op, as today.
2. Marker exists: log Information and return. No `Payment` is created, nothing is published,
   and the offset is committed.
3. Otherwise: run the existing flow (create, evaluate, save, commit, then publish).

Nothing is recorded for the skipped placement. A redelivery hits the marker again, which makes
it idempotent.

**Rationale**: The spec says to create no payment and publish no outcome. Recording a
"placement cancelled" row would only be for audit, and the log line covers that.

## R8. Cancellation when a payment exists

**Decision**: `RefundPaymentHandler` keeps its US-04 logic. The only changes are that it runs
inside the order unit of work, and that the "no payment" branch now looks up or records the
marker instead of returning. `SaveRefundAsync` stays conditional (only if Processed). Under the
lock this is belt and braces.

## R9. Testing the race (SC-002)

**Decision**: Add an integration test on a real PostgreSQL container (`PlaceCancelRaceTests`).
For 50 rounds, it runs `ProcessPaymentHandler` and `RefundPaymentHandler` for the same new order
concurrently, each in its own DI scope and `DbContext`, released together by a barrier. It then
asserts that each order ends in an allowed outcome:

- a marker and no payment;
- a Failed payment and no marker;
- a Refunded payment and no marker.

Rounds use amounts below and above the threshold. Two forced-order tests cover each side
winning. A further test, which swaps in a no-op lock, shows the race reproduces without the
lock. That makes the lock's necessity visible, as Inventory's test does.

## R10. Documentation

**Decision**: Add a new ADR, `docs/adr/0004-per-order-serialisation.md`, because this is a new
locking decision (Constitution: architectural decisions are recorded as ADRs). Update the
following:

- `README.md`: remove the known limitation; describe the marker; add the new setting.
- Consumer, Service, Domain and Infrastructure READMEs.
- `CLAUDE.md`: every handler that reads or changes an order's state takes the order lock first.
