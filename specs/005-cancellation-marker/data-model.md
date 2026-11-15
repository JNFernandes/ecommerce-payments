# Data Model: Do Not Charge an Already-Cancelled Order (US-05)

## Domain (`Ecommerce.Payments.Domain`)

### `Orders/CancellationMarker` (new, sealed class, immutable)

| Property | Type | Notes |
|---|---|---|
| `OrderId` | `Guid` | the cancelled order (`OrderCancelled.aggregateId`); not empty |
| `SourceEventId` | `Guid` | `eventId` of the `OrderCancelled` that created it; not empty |
| `CancelledAt` | `DateTimeOffset` | `OrderCancelled.occurredAt` |
| `RecordedAt` | `DateTimeOffset` | when Payments saved it (`TimeProvider`) |
| `Reason` | `string?` | optional `OrderCancelled.reason` |

- `static CancellationMarker Record(Guid orderId, Guid sourceEventId, DateTimeOffset cancelledAt, DateTimeOffset recordedAt, string? reason)`:
  throws `ArgumentException` for an empty `orderId` or `sourceEventId`.
- `static CancellationMarker Rehydrate(...)`: for persistence only; it runs no validation rules.
- There is no state machine. A marker is created once and is never changed or removed.

`Payment` is unchanged.

## Service (`Ecommerce.Payments.Service`)

| Type | Kind | Change |
|---|---|---|
| `Orders/IOrderUnitOfWork` | interface, new | `Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken)`. Opens a transaction that holds the per-order lock until commit or rollback. |
| `Orders/IOrderTransaction` | interface, new | `Task CommitAsync(CancellationToken)`. `IAsyncDisposable`: disposing without a commit rolls back. |
| `Orders/ICancellationMarkerRepository` | interface, new | `Task<CancellationMarker?> GetByOrderIdAsync(Guid, CancellationToken)`, `Task AddAsync(CancellationMarker, CancellationToken)` |
| `Payments/ProcessPaymentHandler` | changed | Gains `IOrderUnitOfWork` and `ICancellationMarkerRepository`. Each attempt does: lock → payment exists? → marker exists? → create/evaluate → save → commit. Then it publishes. |
| `Payments/RefundPaymentHandler` | changed | Gains `IOrderUnitOfWork` and `ICancellationMarkerRepository`; it keeps using the `TimeProvider` it already has for `RecordedAt`. Each attempt does: lock → payment → (no payment: look up the marker, then record it, or log a duplicate or already-marked) / US-04 path → commit. Then it publishes if refunded. |

`IPaymentRepository` and `IPaymentEventPublisher` are unchanged.

## Infrastructure (`Ecommerce.Payments.Infrastructure`)

### Table `order_cancellation_markers` (new; migration `AddOrderCancellationMarkers`)

| Column | Type | Constraints |
|---|---|---|
| `OrderId` | `uuid` | **PRIMARY KEY**: one marker per order (FR-003) |
| `SourceEventId` | `uuid` | `NOT NULL` |
| `CancelledAt` | `timestamp with time zone` | `NOT NULL` |
| `RecordedAt` | `timestamp with time zone` | `NOT NULL` |
| `Reason` | `text` | nullable |

Column names use PascalCase, following the existing tables (`payments`, `payment_dead_letters`).
`payments` and `payment_dead_letters` are not changed.

### Types

| Type | Kind | Notes |
|---|---|---|
| `Persistence/Entities/CancellationMarkerEntity` | new | EF model; XML docs on every property |
| `Persistence/CancellationMarkerRepository` | new | Explicit mapping to and from `CancellationMarker` (`AsNoTracking` + `Rehydrate` on read) |
| `Persistence/OrderUnitOfWork` (+ private `OrderTransaction`) | new | `BeginTransactionAsync` → `SET LOCAL lock_timeout = <n>ms` → `SELECT pg_advisory_xact_lock(<key>)`. Rollback clears the `ChangeTracker`. |
| `Persistence/OrderLockKey` | new, internal static | `long For(Guid orderId)`: XORs the two 64-bit halves |
| `Persistence/PersistenceOptions` | new | Section `Persistence`; `OrderLockTimeoutMs` (default 5000, > 0) |
| `PaymentsDbContext` | changed | `DbSet<CancellationMarkerEntity> CancellationMarkers`; table mapping |

## What each event writes (inside one locked transaction)

| Event | Situation | payments | order_cancellation_markers | Published | Log |
|---|---|---|---|---|---|
| OrderPlaced | no payment, no marker | + Processed/Failed | — | processed / failed | (existing) |
| OrderPlaced | payment exists | — | — | — | (existing, none) |
| OrderPlaced | marker exists | — | — | — | Info "order already cancelled, not charging" |
| OrderCancelled | Processed payment | → Refunded | — | refunded | (US-04) |
| OrderCancelled | Failed / Pending / Refunded payment | — | — | — | (US-04) Info |
| OrderCancelled | no payment, no marker | — | + marker | — | Info "cancellation recorded before payment" |
| OrderCancelled | no payment, marker with same `eventId` | — | — | — | Info "duplicate delivery" |
| OrderCancelled | no payment, marker with other `eventId` | — | — | — | Info "already marked" |

Invariant (FR-006): for any order, a marker or a payment may exist, never both, as produced by
this service.
