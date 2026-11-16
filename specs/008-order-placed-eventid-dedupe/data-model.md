# Data Model: OrderPlaced eventId Dedupe (US-08)

**No schema change and no migration.** `payments."SourceEventId"` already exists and is filled
for every payment.

## Service (`Ecommerce.Payments.Service`)

| Type | Change |
|---|---|
| `IPaymentRepository` | **Remove** `ExistsByOrderIdAsync`. `GetByOrderIdAsync` (`Payment?`) is used instead. |
| `ProcessPaymentHandler` | Inside the per-order transaction, it loads the payment by order id. Two outcomes are new: `DuplicateDelivery(PaymentId)`, when `SourceEventId` equals the inbound `eventId`, and `AlreadyExists(PaymentId, SourceEventId)`, when the `eventId` is different. Each is logged once at Information. The marker and creation paths are unchanged. |

## Infrastructure (`Ecommerce.Payments.Infrastructure`)

| Type | Change |
|---|---|
| `PaymentRepository` | **Remove** `ExistsByOrderIdAsync`. |

## Decision table (`OrderPlaced`, inside the order transaction)

| Payment for the order | Marker | Result | Writes | Log |
|---|---|---|---|---|
| exists, `SourceEventId == eventId` | — | duplicate delivery | none | Info "duplicate delivery" |
| exists, different `eventId` | — | already exists | none | Info "already exists (created by …)" |
| none | exists | order cancelled (US-05) | none | Info "Not charging order …" (unchanged) |
| none | none | create and evaluate the payment | payment + outbox entry | (unchanged) |
