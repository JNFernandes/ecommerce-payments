# Quickstart: Refund on Order Cancelled (US-04)

Contracts: [orders.order-cancelled](contracts/orders.order-cancelled.md),
[payments.payment-refunded](contracts/payments.payment-refunded.md). Model:
[data-model.md](data-model.md).

## Automated validation

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test          # Docker must be running
```

| Scenario | Test project | Proves |
|---|---|---|
| `RequestRefund`: Processed → Refunded + event; Failed/Pending → Ignored(NotCompleted); Refunded → Ignored(AlreadyRefunded); `Refund` on non-Processed throws, state unchanged | Domain.Tests | FR-003, FR-004 |
| `OrderCancelledEvent.TryParse`: valid, optional fields absent, each required field missing, malformed JSON | Service.Tests | FR-011 |
| Handler: refund saves before publishing; same `eventId` as `RefundSourceEventId` → ignored before Domain; no payment / Failed / Pending / Refunded → no save, no publish, Information log; conditional update lost → no publish; DB failure after retries → throws, no publish | Service.Tests | FR-004..FR-006, FR-010, R3, R4 |
| Consumer base: a message whose handling throws is re-consumed (same offset) after a backoff and committed only once it succeeds; the next message is not committed before it | Component.Tests | FR-010, Constitution VI at-least-once |
| `OrderPlacedConsumer` unchanged after base-class extraction | existing Integration/Component tests | FR-013 |
| Consumer → DB: Processed payment becomes Refunded with `RefundedAt`, `RefundSourceEventId`; ignored cases leave rows unchanged; malformed message committed | Integration.Tests | US1, US2 |
| Concurrent `SaveRefundAsync` for the same payment: exactly one returns true | Integration.Tests | R4, SC-001 |
| Full flow: `OrderPlaced` → Processed; `OrderCancelled` on Kafka → `payments.payment-refunded` with key = payment id and all fields | Component.Tests | US1, FR-007 |
| Redelivered `OrderCancelled` → still exactly one refund message | Component.Tests | SC-003 |
| Publish fails (unreachable broker) → dead-letter row for `payments.payment-refunded`; retry processor republishes it to real Kafka | Component.Tests | FR-008, SC-005 |

## Manual check against ecommerce-infra

1. `dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`
2. `dotnet run --project src/Ecommerce.Payments.Consumer`
3. Place an order through `ecommerce-orders` (or produce an `OrderPlaced` within the threshold)
   and wait for `payments.payment-processed`.
4. Cancel the order in `ecommerce-orders` (or produce an `OrderCancelled` with that order id as
   `aggregateId`).
5. **Expected**: the payment row has `Status = 'Refunded'`, `RefundedAt` and
   `RefundSourceEventId` set; one message on `payments.payment-refunded` keyed by the payment id.
6. Produce the same `OrderCancelled` again → log "already refunded", no new message.
7. Cancel an order whose payment failed (amount over `PaymentPolicy:MaxAmountThreshold`) → log
   "payment not completed", no message.
