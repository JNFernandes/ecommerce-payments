# Quickstart: Validate US-07 (Transactional Outbox)

## Prerequisites

- .NET 10 SDK and Docker (for Testcontainers).
- For the manual check: the `ecommerce-infra` stack, no other local Payments Consumer running,
  and isolated `us07-manual.*` topics (as in US-05 and US-06).

## Automated checks

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

| Scenario | Test |
|---|---|
| Handlers add the outbox entry inside the transaction before commit, and never publish | `ProcessPaymentHandlerTests`, `RefundPaymentHandlerTests` (unit) |
| Commit fails → neither the payment nor the outbox entry exists | `OutboxAtomicityTests` (integration) |
| Claim order per payment; `Exhausted` blocks later entries; concurrent claimers | `OutboxClaimTests` (integration) |
| Migration converts existing dead-letter rows of each status and backfills `EventId` and `Sequence` | `OutboxMigrationTests` (integration) |
| Retry and backoff policy, processor cycle, worker loop | `OutboxRetryPolicyTests`, `OutboxRelayProcessorTests`, `OutboxRelayWorkerTests` (unit) |
| Outcome saved while the relay is down is published once after it starts | `OutboxCrashRecoveryTests` (component) |
| p95 latency under 1 s when healthy | `OutboxLatencyTests` (component) |
| Processed and refunded published in order after a broker outage | `OutboxOrderingTests` (component) |
| Existing flows (process, fail, refund, duplicate, cancel-before-place) | existing component tests, with the relay hosted |

## Manual check (ecommerce-infra)

1. Apply the migration:
   `dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`.
   Expect `payment_dead_letters` to become `payment_outbox`, with any earlier rows kept and
   mapped to the new statuses.
2. Run the Consumer on isolated topics and its own group. Produce an `OrderPlaced`.
   - Expect a `payment_outbox` row that goes from `Pending` to `Published` within about 1 s.
   - Expect one message on the isolated `payments.payment-processed` topic.
3. Stop the Consumer, produce an `OrderPlaced`, then start the Consumer again. Do the same with
   the broker unreachable, then restore it. Expect each event published exactly once once
   everything is back.
4. Clean up the topics, the group and the test rows.
