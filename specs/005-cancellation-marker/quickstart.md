# Quickstart: Validate US-05 (Do Not Charge an Already-Cancelled Order)

## Prerequisites

- .NET 10 SDK and Docker running (Testcontainers).
- For the manual check: the `ecommerce-infra` stack up (Kafka `localhost:9092`, Postgres
  `localhost:5434`), and **no other local Payments Consumer running**, because it would compete
  in the same consumer group.

## Automated checks

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

The tests to look for:

| Scenario | Test |
|---|---|
| Marker recorded; duplicate and already-marked cancellations | `RefundPaymentHandlerTests`, `CancellationBeforePaymentTests` (integration) |
| OrderPlaced for a marked order creates and publishes nothing | `ProcessPaymentHandlerTests`, `CancellationBeforePaymentTests` |
| Per-order lock: same order waits, different orders don't, timeout, rollback, clean retry | `OrderUnitOfWorkTests` (integration) |
| 50 concurrent rounds end in an allowed outcome; each side can win | `PlaceCancelRaceTests` (integration) |
| Without the lock the race reproduces | `PlaceCancelRaceTests` (no-op lock variant) |
| End to end on Kafka: cancel first → place → no payment, no events | `CancelBeforePlaceFlowTests` (component) |
| Migration creates the table with its primary key | `CancellationMarkerMigrationTests` (integration) |

## Manual check (ecommerce-infra)

1. Apply the migration: `dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`.
2. Start the service: `dotnet run --project src/Ecommerce.Payments.Consumer`.
3. Produce an `OrderCancelled` for a new order id X to `orders.order-cancelled` (envelope as in
   the [US-04 contract](../004-refund-on-order-cancelled/contracts/orders.order-cancelled.md)).
   - Expect: an Information log saying the cancellation was recorded before the payment, and a
     row for X in `order_cancellation_markers`.
4. Produce an `OrderPlaced` for X to `orders.order-placed`.
   - Expect: an Information log saying order X is already cancelled and is not being charged;
     no row for X in `payments`; nothing on `payments.payment-processed` or
     `payments.payment-failed` for X.
5. Produce the step 3 message again.
   - Expect: a "duplicate delivery" log line and no change.

See [data-model.md](data-model.md) for every case and [contracts/](contracts/) for the
promised behaviour.
