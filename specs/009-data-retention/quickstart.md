# Quickstart: Validate US-09 (Data Retention)

## Prerequisites

- .NET 10 SDK.
- Docker, for Testcontainers.
- There is no `contracts/` folder, because nothing external changes.

## Automated checks

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

| Scenario | Test |
|---|---|
| Options validation; worker first run, interval, failure retry, log level | `RetentionOptionsTests`, `RetentionWorkerTests` (unit) |
| Only expired rows are purged; `Pending` and `Exhausted` entries are kept; a store set to 0 is untouched | `RetentionPurgerTests` (integration) |
| A backlog larger than 10 batches is fully purged; two purgers at once delete everything expired with no errors | `RetentionPurgerTests` |
| Purging a published earlier entry leaves the later `Pending` entry claimable | `RetentionPurgerTests` |
| The migration creates the three indexes | `RetentionPurgerTests` |

## Manual check (ecommerce-infra, optional)

1. Apply the migration:

   ```powershell
   dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure
   ```

2. Insert test rows whose times are older than the retention periods: a published outbox entry,
   a dead letter and a marker. Insert one recent row of each as well.
3. Start the Consumer with `Retention__IntervalMs=10000`. Use isolated topics, as in US-05 to
   US-07.
4. Expect:
   - an Information line, "Retention purge deleted 1 outbox entries, 1 dead letters, 1
     cancellation markers";
   - the old rows are gone and the recent ones remain.
5. Delete the test rows and topics.
