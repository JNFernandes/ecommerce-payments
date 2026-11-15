# Implementation Plan: Inbound Dead-Letter for Messages That Keep Failing

**Branch**: `feature/US-06-inbound-dead-letter` | **Date**: 2026-11-15 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/006-inbound-dead-letter/spec.md`

## Summary

`KafkaConsumerBase` already retries a failing message in place, with capped backoff, and today
it does so forever. It will now count attempts per message. Once a message reaches
`Kafka:ConsumerMaxAttempts` (default 5), the base stores the raw message in a new
`consumed_dead_letters` table, logs an Error and commits the offset. The table holds the topic,
partition, offset, key, payload, error, attempts and time, with a unique index on
(Topic, Partition, Offset), and is written by an idempotent `INSERT … ON CONFLICT DO NOTHING`.
If the store itself fails, the base commits nothing and keeps retrying, handling the message
first each time.

Malformed envelopes also go to the table. The consumers throw a new `InvalidMessageException`,
and the base stores the message once with attempts 1 and commits, without retrying.

Replay is a documented manual procedure. There is one migration and no new topics, events or
Domain or Service changes. This closes the open Constitution VI item carried over from US-04,
recorded in a new ADR 0005.

## Technical Context

**Language/Version**: C# / .NET 10

**Primary Dependencies**: Confluent.Kafka 2.15.0, EF Core 10.0.4 + Npgsql 10.0.2. No new
packages.

**Storage**: PostgreSQL. New table `consumed_dead_letters`; existing tables are unchanged.

**Testing**: xUnit, Testcontainers (PostgreSQL, Kafka)

**Target Platform**: .NET Worker Service

**Project Type**: Event-driven background worker, no HTTP surface

**Performance Goals**:
- No cost on the success path beyond tracking one integer and one offset.
- A message that always fails is parked about 15 s after its first attempt, with the defaults.

**Constraints**:
- Never commit before the dead-letter row is stored.
- Never lose a message.
- Shutdown is not counted as a failure.
- Startup still does not depend on Kafka.

**Scale/Scope**: the shared consumer loop, two consumers, one table, one setting and the docs.

There are no open NEEDS CLARIFICATION items; see [research.md](research.md).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Checked against v1.2.0.

| Principle | Check | Status |
|---|---|---|
| I. Layering / Consumer duties | The Consumer "MUST catch all exceptions … logged and routed to retry/dead-letter" and owns offset commits. The routing and the store call live in `KafkaConsumerBase`. The store is an Infrastructure persistence port used only for this transport-level record. No business state is written, and the Service and Domain layers are untouched. | ✅ (see the note below) |
| II. Write flow | No `Payment` or marker writes, and no publishing. Commit happens only after the handler succeeds or the dead-letter row is stored. A failed store means no commit. | ✅ |
| II. Idempotency | Unique (Topic, Partition, Offset) plus `ON CONFLICT DO NOTHING`. Replays rely on the existing idempotency of the handlers. | ✅ |
| III. Type safety | Typed `ConsumedDeadLetter` record and `InvalidMessageException`. The raw payload is stored as text and never passed on as `object` or `dynamic`. | ✅ |
| IV/V. Tests | No Domain or Service methods change. Component tests cover the base loop with real Kafka and real consumers. Integration tests cover the store and the migration. | ✅ |
| VI. "Retried with exponential backoff before being routed to a dead-letter table" | Implemented here: backoff for `ConsumerMaxAttempts`, then `consumed_dead_letters`. This closes the item carried over from US-04. | ✅ |
| VI. At-least-once | The offset is committed only after a durable handle or a durable store. A store failure keeps the message. | ✅ |
| VI. Startup and broker availability | Unchanged (`Task.Yield` before subscribe). | ✅ |
| VII. Branching | `feature/US-06-inbound-dead-letter`, branched from an up-to-date `main`. Commits use `feat(US-06)`, `test(US-06)` and `docs(US-06)`. | ✅ |
| VIII. Quality gate | Build, then format, then test, after every task. | ✅ |
| Docs | ADR 0005. Root README (background-services behaviour, known limitations, tables, configuration). Consumer and Infrastructure READMEs, `CLAUDE.md`. New `docs/consumed-dead-letters.md` (query and replay). XML docs on new public types. | ✅ |

**Note on Principle I**: the Consumer calls an Infrastructure store directly rather than going
through the Service layer. This is deliberate. The constitution assigns dead-letter routing to
the Consumer, and the record is about a Kafka position, not about a payment. Sending it through
the Service would give the Service knowledge of Kafka partitions and offsets, which Principle I
keeps out of the layers below the Consumer.

**Post-design re-check**: unchanged, all ✅.

## Design

### Consume loop (`KafkaConsumerBase.ExecuteAsync`)

```
consume → result (key, value, topic/partition/offset)
if result.TopicPartitionOffset != failing.Position: failing = (position, attempts 0)
try
  if HandleMessageAsync(value) → Commit(result)
  reset failing
catch OperationCanceledException when stopping → break
catch InvalidMessageException ex
  StoreThenCommitOrRetry(result, ex, attempts: 1, level: Warning)
catch Exception ex
  failing.attempts++
  if failing.attempts >= ConsumerMaxAttempts
      StoreThenCommitOrRetry(result, ex, failing.attempts, level: Error)
  else
      log Error "retrying"; Seek(result); delay(backoff(attempts))

StoreThenCommitOrRetry:
  try   scope → IConsumedDeadLetterStore.StoreAsync(...); Commit(result); log; reset failing
  catch storeEx → log Error "could not dead-letter …; not committed, retrying";
                  Seek(result); delay(backoff(max(attempts, 1)))
```

### Components

| Component | Project / path | Kind |
|---|---|---|
| `ConsumedDeadLetterEntity`, `ConsumedDeadLetter`, `IConsumedDeadLetterStore`, `ConsumedDeadLetterStore` | Infrastructure `Persistence/` | new |
| `PaymentsDbContext` (+ `ConsumedDeadLetters`) | Infrastructure `Persistence/` | extend |
| Migration `AddConsumedDeadLetters` | Infrastructure `Persistence/Migrations/` | new |
| `KafkaOptions.ConsumerMaxAttempts` | Infrastructure `Messaging/` | extend |
| `InvalidMessageException` | Consumer `Consumers/` | new |
| `KafkaConsumerBase` | Consumer `Consumers/` | change |
| `OrderPlacedConsumer`, `OrderCancelledConsumer` | Consumer `Consumers/` | change (throw on malformed) |
| DI: `IConsumedDeadLetterStore`; `KafkaOptions` validation; `appsettings.json` `Kafka:ConsumerMaxAttempts` | Consumer `Program.cs` | extend |
| ADR 0005, `docs/consumed-dead-letters.md`, READMEs, `CLAUDE.md` | docs | new / extend |

## Project Structure

### Documentation (this feature)

```text
specs/006-inbound-dead-letter/
├── spec.md  plan.md  research.md  data-model.md  quickstart.md
├── contracts/consumed-dead-letters.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
src/
├── Ecommerce.Payments.Infrastructure/
│   ├── Persistence/Entities/ConsumedDeadLetterEntity.cs                          (new)
│   ├── Persistence/ConsumedDeadLetter.cs  IConsumedDeadLetterStore.cs
│   │   ConsumedDeadLetterStore.cs                                                (new)
│   ├── Persistence/PaymentsDbContext.cs                                          (extend)
│   ├── Persistence/Migrations/<timestamp>_AddConsumedDeadLetters.cs              (new)
│   └── Messaging/KafkaOptions.cs                                                 (extend)
└── Ecommerce.Payments.Consumer/
    ├── Consumers/InvalidMessageException.cs                                      (new)
    ├── Consumers/KafkaConsumerBase.cs  OrderPlacedConsumer.cs
    │   OrderCancelledConsumer.cs                                                 (change)
    └── Program.cs  appsettings.json  README.md                                   (extend)

tests/
├── Ecommerce.Payments.Integration.Tests/ConsumedDeadLetterStoreTests.cs          (new)
├── Ecommerce.Payments.Integration.Tests/ConsumedDeadLetterMigrationTests.cs      (new)
├── Ecommerce.Payments.Integration.Tests/*  (malformed-message tests now expect InvalidMessageException)
└── Ecommerce.Payments.Component.Tests/KafkaConsumerBaseDeadLetterTests.cs        (new)

docs/adr/0005-inbound-dead-letter.md  docs/consumed-dead-letters.md               (new)
```

**Structure Decision**: keep the existing projects and add none.

## Risks

- **Behaviour change for malformed messages**: they used to be logged and committed; now they
  are stored, then committed. The existing tests are updated to expect `InvalidMessageException`
  from `HandleMessageAsync`.
- **Skipping a stored message breaks per-partition ordering for that order.** This is accepted:
  the events are idempotent and order-tolerant (US-05), and an operator replays the stored
  message.
- **Key type change** (`Ignore` to `string?`): a non-UTF-8 key would be decoded loosely. Orders
  uses UTF-8 string keys.
- **Database down for a long time**: both handling and storing fail, so the partition stays
  blocked as it does today. This is correct, because nothing could be processed anyway.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Pre-existing: `OrderPlacedConsumer` dedupes by order id, not `eventId` | Unchanged by this story | Still a separate follow-up |
