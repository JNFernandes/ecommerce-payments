# Quickstart: Validate US-08 (OrderPlaced eventId Dedupe)

## Prerequisites

You need the .NET 10 SDK and Docker, which the Testcontainers tests use. US-08 has no Kafka
contracts and no schema change, so there is no `contracts/` folder and no migration.

## Automated checks

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

| Scenario | Test |
|---|---|
| The same `eventId` is handled as a duplicate delivery: nothing is written and one Information line is logged | `ProcessPaymentHandlerTests` (unit) |
| A different `eventId` for an existing payment is handled as "already exists": nothing is written and one Information line is logged | `ProcessPaymentHandlerTests` (unit) |
| The same message twice, or a new `eventId` for the same order, leaves one payment and one outbox entry | `OrderPlacedToPaymentTests` (integration) |
| A redelivery end to end on Kafka | `DuplicateDeliveryTests` (component, unchanged) |

## Manual check (optional)

1. On isolated topics, as in US-05 to US-07, produce the same `OrderPlaced` twice.
2. Expect one payment and one `PaymentProcessed` message.
3. Expect the second delivery to log "Ignoring OrderPlaced … duplicate delivery".
