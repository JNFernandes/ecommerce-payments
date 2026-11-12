# Quickstart: Dead-Letter Retry (US-03)

Validation guide proving the feature works. Contracts: [contracts/dead-letter-retry.md](contracts/dead-letter-retry.md);
schema and states: [data-model.md](data-model.md).

## Prerequisites

- .NET 10 SDK, Docker running.
- For the manual scenario: `ecommerce-infra` up (`docker compose up -d` in `../ecommerce-infra`),
  Kafka on `localhost:9092`, Postgres on `localhost:5434`.

## 1. Automated validation

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

| Scenario | Test project | Proves |
|---|---|---|
| Backoff doubles from base, capped at max | Infrastructure.Tests (`DeadLetterRetryPolicyTests`) | FR-008 |
| Failure at the last allowed retry → Exhausted | Infrastructure.Tests | FR-009, US2 |
| Success → Resolved with `ResolvedAt` = fake clock now | Infrastructure.Tests (`DeadLetterRetryProcessorTests`) | FR-007 |
| Retry failure updates the row, never creates one | Infrastructure.Tests | FR-006 |
| Not-yet-due / Resolved / Exhausted rows are not claimed | Integration.Tests (`DeadLetterClaimTests`) | US1 #2–3 |
| Two concurrent claimers get disjoint rows, no waiting | Integration.Tests | FR-010, US3 |
| Migration backfills existing rows (PendingRetry, topic, key) | Integration.Tests | FR-002 |
| First publish fails (unreachable broker) → row; worker republishes to real Kafka → Resolved, same `eventId` and key | Component.Tests (`DeadLetterRetryFlowTests`) | US1, SC-006 |
| Worker attempt fails then succeeds after the backoff | Component.Tests | US2 #1, US1 |

## 2. Manual scenario against ecommerce-infra

1. Apply the migration:
   `dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`
2. Insert a test row with the example SQL in `docs/dead-letter-retry.md`.
3. Run the worker with a short interval:
   ```powershell
   $env:DeadLetterRetry__IntervalMs = "5000"
   dotnet run --project src/Ecommerce.Payments.Consumer
   ```
4. **Expected**: within ~5 s the log shows the republish; the row has `Status = 'Resolved'` and
   `ResolvedAt` set; the message is on `payments.payment-processed` with the inserted `eventId`.
5. Outage check: stop Kafka (`docker stop` the broker), insert another row, watch `RetryCount`
   grow and `NextRetryAt` move out by 5 s, 10 s, 20 s…; restart Kafka and see it resolve.
6. Startup check: with Kafka stopped, start the service — it starts normally and logs failed
   attempts instead of crashing.
