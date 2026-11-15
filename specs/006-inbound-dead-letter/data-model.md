# Data Model: Inbound Dead-Letter (US-06)

No Domain or Service changes. `Payment`, `CancellationMarker` and the handlers are unchanged.

## Table `consumed_dead_letters` (new; migration `AddConsumedDeadLetters`)

| Column | Type | Constraints |
|---|---|---|
| `Id` | `uuid` | PK |
| `Topic` | `text` | `NOT NULL` |
| `Partition` | `integer` | `NOT NULL` |
| `Offset` | `bigint` | `NOT NULL` |
| `MessageKey` | `text` | nullable |
| `Payload` | `text` | nullable (raw value as received, even if not JSON) |
| `Error` | `text` | `NOT NULL` (`Type: message`, plus the innermost exception if different) |
| `Attempts` | `integer` | `NOT NULL`, `>= 1` (1 = malformed) |
| `FailedAt` | `timestamp with time zone` | `NOT NULL` (`TimeProvider`) |
| | | unique index `IX_consumed_dead_letters_position` on (`Topic`, `Partition`, `Offset`) |

Column names are PascalCase, like the other tables. Rows are insert-only; nothing in the
service updates or deletes them.

`payment_dead_letters` (outbound, US-03) is unchanged.

## Types

| Type | Project / path | Kind | Notes |
|---|---|---|---|
| `ConsumedDeadLetterEntity` | Infrastructure `Persistence/Entities/` | new | EF model, used for the mapping, migration and tests; XML docs |
| `ConsumedDeadLetter` | Infrastructure `Persistence/` | new, record | `Topic`, `Partition`, `Offset`, `MessageKey?`, `Payload?`, `Error`, `Attempts` (input to the store) |
| `IConsumedDeadLetterStore` | Infrastructure `Persistence/` | new | `Task StoreAsync(ConsumedDeadLetter, CancellationToken)`; idempotent per position |
| `ConsumedDeadLetterStore` | Infrastructure `Persistence/` | new | `INSERT … ON CONFLICT DO NOTHING` via `ExecuteSqlAsync`; `FailedAt` from `TimeProvider`; new `Id` |
| `PaymentsDbContext` | Infrastructure `Persistence/` | extend | `DbSet<ConsumedDeadLetterEntity> ConsumedDeadLetters`; table, unique index and `Attempts` check constraint |
| `KafkaOptions` | Infrastructure `Messaging/` | extend | `ConsumerMaxAttempts` (default 5) |
| `InvalidMessageException` | Consumer `Consumers/` | new | `Reason`; thrown by consumers for a malformed envelope; not retried |
| `KafkaConsumerBase` | Consumer `Consumers/` | change | `IConsumer<string?, string?>`; attempt tracking; dead-letter on max or invalid; store-failure fallback |
| `OrderPlacedConsumer`, `OrderCancelledConsumer` | Consumer `Consumers/` | change | Throw `InvalidMessageException` instead of returning `true` for a malformed envelope |

## Consume-loop decisions

| Situation | Store? | Commit? | Log |
|---|---|---|---|
| Handled | — | yes | (handler's own) |
| `InvalidMessageException` | yes, attempts 1 | yes, after the store | Warning "malformed … stored" |
| `InvalidMessageException`, store fails | tried | **no**, seek back plus backoff | Error "could not dead-letter" |
| Other exception, attempts < max | — | no, seek back plus backoff | Error "failed … retrying" (existing) |
| Other exception, attempts ≥ max | yes, attempts = n | yes, after the store | Error "dead-lettered after n attempts" |
| Other exception, attempts ≥ max, store fails | tried | **no**, seek back plus backoff | Error "could not dead-letter" |
| Shutdown (`OperationCanceledException` with the stopping token) | — | no | — |
