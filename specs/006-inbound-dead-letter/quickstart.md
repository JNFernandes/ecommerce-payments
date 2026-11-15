# Quickstart: Validate US-06 (Inbound Dead-Letter)

## Prerequisites

- .NET 10 SDK, with Docker running for Testcontainers.
- For the manual check:
  - the `ecommerce-infra` stack, with no other local Payments Consumer running;
  - isolated topics, as in the US-05 manual check, so other services are not affected.

## Automated checks

```powershell
dotnet build
dotnet format --verify-no-changes
dotnet test
```

| Scenario | Test |
|---|---|
| Message that always fails is stored after the maximum number of attempts; the next message is then handled; the offset is committed past both | `KafkaConsumerBaseDeadLetterTests` (component) |
| Store fails, so nothing is committed; once the store recovers, the message is stored and committed | `KafkaConsumerBaseDeadLetterTests` |
| A message that succeeds before the maximum stores nothing (US-04 test unchanged) | `KafkaConsumerBaseRetryTests` |
| Malformed `OrderPlaced` and `OrderCancelled` are stored with attempts 1 and not retried | `KafkaConsumerBaseDeadLetterTests` (real consumers) |
| Same position stored twice gives one row; column values stored as received | `ConsumedDeadLetterStoreTests` (integration) |
| Migration creates the table and unique index | `ConsumedDeadLetterMigrationTests` (integration) |

## Manual check (ecommerce-infra)

1. Apply the migration with
   `dotnet ef database update --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure`.
2. Start the Consumer on isolated topics and its own consumer group. Use environment overrides of
   `Kafka:*Topic` and `Kafka:ConsumerGroupId`, and create the topics by hand.
3. Produce `not json` to the isolated `orders.order-placed` topic.
   - Expect a Warning log line.
   - Expect one row in `consumed_dead_letters` with `Attempts = 1` and `Payload = 'not json'`.
4. Replay a valid `OrderPlaced` with the procedure in `docs/consumed-dead-letters.md`.
   - Expect it to be processed normally.
5. Clean up: stop the Consumer, then delete the topics, the consumer group and the test rows.

A message that always fails, as opposed to one that is malformed, is covered by the component
tests. Reproducing one by hand would need a database fault in the shared stack.
