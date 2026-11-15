---

description: "Task list for US-06 Inbound Dead-Letter"
---

# Tasks: Inbound Dead-Letter for Messages That Keep Failing

**Input**: Design documents from `specs/006-inbound-dead-letter/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md,
contracts/consumed-dead-letters.md, quickstart.md (all present).

**Tests**: Included and REQUIRED. Constitution Principles IV and V require integration and
component coverage, and quickstart.md maps each scenario to a test. There are no Domain or
Service changes, so no new unit tests are needed there.

**Organization**: grouped by the user stories in spec.md:
- US1: a message that keeps failing stops blocking its partition (P1).
- US2: malformed messages are stored (P2).
- US3: operators can find and replay stored messages (P3).

**No Setup phase**: no new projects or packages.

**Quality gate**: after **every** task, run in order:
1. `dotnet build`
2. `dotnet format --verify-no-changes`
3. `dotnet test`

This is Constitution VIII. Docker must be running for the tests.

**Commits**: Conventional Commits scoped `US-06`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependency on an incomplete task)
- **[Story]**: US1, US2 or US3
- **(new)**: the file does not exist yet

---

## Phase 1: Foundational (Blocking Prerequisites)

**Purpose**: the table, the idempotent store, the new setting, the exception type and the
consumer key change. Behaviour does not change yet.

**⚠️ CRITICAL**: no user story work can begin until this phase is complete.

- [X] T001 [P] Create `ConsumedDeadLetterEntity` **(new)** in `src/Ecommerce.Payments.Infrastructure/Persistence/Entities/ConsumedDeadLetterEntity.cs`.
  - Properties, each with an XML doc: `Id` (`Guid`), `Topic` (`string`), `Partition` (`int`), `Offset` (`long`), `MessageKey` (`string?`), `Payload` (`string?`), `Error` (`string`), `Attempts` (`int`), `FailedAt` (`DateTimeOffset`).
  - In `src/Ecommerce.Payments.Infrastructure/Persistence/PaymentsDbContext.cs`:
    - add `DbSet<ConsumedDeadLetterEntity> ConsumedDeadLetters`;
    - map it with `ToTable("consumed_dead_letters", t => t.HasCheckConstraint("CK_consumed_dead_letters_attempts", "\"Attempts\" >= 1"))` and `HasKey(d => d.Id)`;
    - add a unique index on `{ Topic, Partition, Offset }` named `IX_consumed_dead_letters_position`;
    - update the class XML doc.
- [X] T002 Generate the migration `AddConsumedDeadLetters`, depending on T001:
  - run `dotnet ef migrations add AddConsumedDeadLetters --project src/Ecommerce.Payments.Infrastructure --startup-project src/Ecommerce.Payments.Infrastructure --output-dir Persistence/Migrations`;
  - run `dotnet format`;
  - verify the migration only creates the table, the unique index and the check constraint.
- [X] T003 Create the store, depending on T001:
  - **Record (new)**: `public sealed record ConsumedDeadLetter(string Topic, int Partition, long Offset, string? MessageKey, string? Payload, string Error, int Attempts)` in `src/Ecommerce.Payments.Infrastructure/Persistence/ConsumedDeadLetter.cs`, with XML docs.
  - **Interface (new)**: `IConsumedDeadLetterStore` in `src/Ecommerce.Payments.Infrastructure/Persistence/IConsumedDeadLetterStore.cs`, with `Task StoreAsync(ConsumedDeadLetter deadLetter, CancellationToken)`. Its doc says the call is idempotent per position, the caller commits the offset only after it returns, and it throws when the row could not be stored.
  - **Implementation (new)**: `ConsumedDeadLetterStore(PaymentsDbContext, TimeProvider)` in `src/Ecommerce.Payments.Infrastructure/Persistence/ConsumedDeadLetterStore.cs`.
    - It runs `ExecuteSqlAsync($"INSERT INTO consumed_dead_letters (\"Id\",\"Topic\",\"Partition\",\"Offset\",\"MessageKey\",\"Payload\",\"Error\",\"Attempts\",\"FailedAt\") VALUES (...) ON CONFLICT (\"Topic\",\"Partition\",\"Offset\") DO NOTHING")`, with every value parameterised.
    - `Id` is `Guid.NewGuid()` and `FailedAt` is `TimeProvider.GetUtcNow()`.
  - Add `services.AddConsumedDeadLetterStore()` (scoped) to a new `src/Ecommerce.Payments.Infrastructure/Persistence/ConsumedDeadLetterServiceCollectionExtensions.cs`.
- [X] T004 [P] Integration tests **(new)**, on a PostgreSQL container with migrations applied, depending on T002 and T003:
  - In `tests/Ecommerce.Payments.Integration.Tests/ConsumedDeadLetterStoreTests.cs`:
    - `Stores_Every_Field_As_Received`. Use a non-JSON payload, a null key and a `FakeTimeProvider` starting on whole seconds.
    - `Storing_Same_Position_Twice_Keeps_One_Row`. The second call does not throw, and the first row's values are kept.
    - `Different_Offsets_Are_Separate_Rows`.
    - `Null_Payload_Is_Stored`.
  - In `tests/Ecommerce.Payments.Integration.Tests/ConsumedDeadLetterMigrationTests.cs`:
    - the table and the unique index exist;
    - a raw insert with `Attempts = 0` violates the check constraint (SqlState `23514`).
- [X] T005 [P] Add the setting and its validation:
  - Add `ConsumerMaxAttempts` (default 5) to `src/Ecommerce.Payments.Infrastructure/Messaging/KafkaOptions.cs`, with an XML doc: "failed handling attempts before a message is stored in `consumed_dead_letters` and committed; > 0".
  - In `src/Ecommerce.Payments.Consumer/Program.cs`, chain `.Validate(o => o.ConsumerMaxAttempts > 0 && o.ConsumerRetryBaseDelayMs > 0 && o.ConsumerRetryMaxDelayMs >= o.ConsumerRetryBaseDelayMs, "Kafka consumer retry settings must be positive and ConsumerRetryMaxDelayMs >= ConsumerRetryBaseDelayMs.").ValidateOnStart()` onto `AddOptions<KafkaOptions>()`, and register `AddConsumedDeadLetterStore()`.
  - Add `"ConsumerMaxAttempts": 5` to the `Kafka` section of `src/Ecommerce.Payments.Consumer/appsettings.json`.

  Depends on T003 for the DI line.
- [X] T006 [P] Create `InvalidMessageException` **(new)** in `src/Ecommerce.Payments.Consumer/Consumers/InvalidMessageException.cs`.
  - `public sealed class InvalidMessageException : Exception` with constructor `(string reason)`, a `Reason` property, and the message `"Malformed message: {reason}"`.
  - XML doc: thrown by a consumer's `HandleMessageAsync` for an envelope that can never be handled; `KafkaConsumerBase` stores it with attempts 1 and does not retry it.
- [X] T007 In `src/Ecommerce.Payments.Consumer/Consumers/KafkaConsumerBase.cs`, change the consumer to `IConsumer<string?, string?>`, built with `ConsumerBuilder<string?, string?>`. Pass `consumeResult.Message.Value ?? string.Empty` to `HandleMessageAsync`. Behaviour must not change otherwise, and all existing tests must stay green.

**Checkpoint**: the table and store exist and are proven by T004; the setting is validated; the
consumer can read keys. The quality gate is green.

---

## Phase 2: User Story 1 — A message that keeps failing no longer blocks its partition (P1) 🎯 MVP

**Goal**: after `ConsumerMaxAttempts` failed attempts, the message is stored, an Error is
logged and the offset is committed, so later messages flow. If the store fails, nothing is
committed and the message is retried.

**Independent Test**: produce a message whose handling always fails, followed by a valid one.
The first is stored with attempts = max, the second is handled, and the committed offset ends
after both.

### Tests for User Story 1

- [ ] T008 [US1] Component tests **(new)** in `tests/Ecommerce.Payments.Component.Tests/KafkaConsumerBaseDeadLetterTests.cs`, on a Kafka container.
  - **Setup**:
    - Use a test subclass of `KafkaConsumerBase`, modelled on `FailFirstAttemptsConsumer` in `KafkaConsumerBaseRetryTests.cs`, that always throws for message "A" and records every call.
    - Use a `RecordingConsumedDeadLetterStore : IConsumedDeadLetterStore`, registered as a singleton in the test `ServiceCollection`. It can be told to throw for its first *k* calls, and it records successful calls.
    - Use `ConsumerMaxAttempts = 3`, `ConsumerRetryBaseDelayMs = 50` and `ConsumerRetryMaxDelayMs = 200`.
  - **Tests**:
    - `Message_That_Always_Fails_Is_Stored_After_Max_Attempts_And_Next_Message_Is_Handled`:
      - produce A then B, both with key `"k1"`;
      - A is handled exactly 3 times, then B;
      - one store call for A with `Attempts = 3`, `Topic`, `Partition` 0, `Offset` 0, `MessageKey = "k1"`, `Payload = "A"`, and an `Error` containing `InvalidOperationException` and the message;
      - the committed offset is 2.
    - `Store_Failure_Keeps_Message_Uncommitted_And_Retries_Until_Stored`:
      - the store throws on its first 2 calls;
      - A keeps being handled (more than 3 calls);
      - the committed offset stays 0 while the store fails;
      - eventually exactly one successful store happens, with `Attempts >= 3`, and the committed offset passes A.
    - `Message_That_Recovers_Before_Max_Is_Not_Stored`:
      - A fails twice and then succeeds, with max 3;
      - no store calls, and the committed offset is 2.

### Implementation for User Story 1

- [ ] T009 [US1] Implement the attempt tracking and dead-lettering in `src/Ecommerce.Payments.Consumer/Consumers/KafkaConsumerBase.cs`, following plan.md "Consume loop" and data-model.md "Consume-loop decisions".
  1. Track the failing message's `TopicPartitionOffset` with its attempt count. Reset the count on a different position, on success and after a successful store.
  2. On a non-cancellation exception, increment the count. While it is below `ConsumerMaxAttempts`, keep the existing log, seek and backoff.
  3. Once the count reaches the maximum, call a private `StoreThenCommitAsync`. It creates a scope, resolves `IConsumedDeadLetterStore`, stores the message, then calls `Commit(consumeResult)`. It logs Error: "Dead-lettered message on {Topic} [{Partition}] @ {Offset} after {Attempts} attempts: {Error}; committed."
  4. If the store throws, log Error with the store exception: "Could not dead-letter message on {Topic} [{Partition}] @ {Offset}; not committed, retrying." Then seek back and wait the backoff for the current count.
  5. Build the error text as research R7 describes, in a `static string DescribeError(Exception)`.
  6. Update the class XML doc. It should cover the new bullets and cite ADR 0005.

  Makes T008 pass. Run the existing `KafkaConsumerBaseRetryTests` too; they must stay green.

**Checkpoint**: one message that keeps failing can no longer stall a partition, and nothing is
lost when the database is down.

---

## Phase 3: User Story 2 — Malformed notifications are kept for review (P2)

**Goal**: malformed envelopes are stored with attempts 1 and committed, without retry.

**Independent Test**: send a malformed `OrderPlaced` and a malformed `OrderCancelled`. Each is
stored once with attempts 1 and the validation error, and both offsets are committed.

### Tests for User Story 2

- [ ] T010 [P] [US2] Update the existing tests:
  - `tests/Ecommerce.Payments.Integration.Tests/OrderCancelledToRefundTests.cs`: rename `HandleMessageAsync_MalformedMessage_IsCommitted` to `HandleMessageAsync_MalformedMessage_Throws_InvalidMessageException`, and assert `await Assert.ThrowsAsync<InvalidMessageException>(...)` and that no refunds were published.
  - `tests/Ecommerce.Payments.Integration.Tests/CancellationBeforePaymentTests.cs`: in `Malformed_OrderCancelled_Records_No_Marker`, assert `ThrowsAsync<InvalidMessageException>` and still no marker.
  - In `tests/Ecommerce.Payments.Component.Tests/KafkaConsumerBaseDeadLetterTests.cs`, add `Invalid_Message_Is_Stored_Once_With_One_Attempt_Without_Retry`. The test consumer throws `InvalidMessageException("missing eventId")` for message "M". Expect it handled exactly once, one store with `Attempts = 1` and an `Error` containing "missing eventId", the next message handled, and the committed offset after both.
  - In the same file, add `Invalid_Message_Store_Failure_Is_Retried`: the store fails once, then succeeds with `Attempts = 1`, and the offset is committed only after that.
- [ ] T011 [P] [US2] Component test **(new)** `tests/Ecommerce.Payments.Component.Tests/MalformedMessageDeadLetterFlowTests.cs`, with PostgreSQL and Kafka containers, both real consumers hosted and the real `ConsumedDeadLetterStore`. DI is set up as in `CancelBeforePlaceFlowTests.cs`, plus `AddConsumedDeadLetterStore()`.
  - Produce `not json` with key `"order-1"` to `orders.order-placed`, and `{"eventId":"…","version":1}` to `orders.order-cancelled`.
  - Poll `consumed_dead_letters` until it has 2 rows. Each must have `Attempts = 1`, the correct `Topic`, the raw `Payload` and an `Error` containing the parse or validation error. The `MessageKey` is `"order-1"` for the first.
  - Then produce a valid `OrderPlaced` and assert that it is processed (a `Processed` payment), which shows the partition was not blocked.

### Implementation for User Story 2

- [ ] T012 [US2] Make the consumers and the base treat malformed messages as dead letters:
  - In `src/Ecommerce.Payments.Consumer/Consumers/OrderPlacedConsumer.cs` and `src/Ecommerce.Payments.Consumer/Consumers/OrderCancelledConsumer.cs`, when `TryParse` fails, `throw new InvalidMessageException(error!)` instead of logging and returning `true`. Update the XML docs.
  - In `src/Ecommerce.Payments.Consumer/Consumers/KafkaConsumerBase.cs`, catch `InvalidMessageException` before the general catch. Call `StoreThenCommitAsync` with attempts 1 and log at **Warning**: "Malformed message on {Topic} [{Partition}] @ {Offset}: {Reason}; stored in consumed_dead_letters and committed." There is no backoff on the first store. If the store fails, use the T009 store-failure path.

  Makes T010 and T011 pass.

**Checkpoint**: every inbound message is handled, stored, or still being retried. None is
dropped.

---

## Phase 4: User Story 3 — Operators can find and replay dead-lettered notifications (P3)

**Goal**: a documented, tested procedure to list stored messages and replay one.

**Independent Test**: follow the document to replay a stored `OrderPlaced`, and check that it is
processed exactly once.

- [ ] T013 [US3] Write `docs/consumed-dead-letters.md` **(new)**. It covers:
  - what lands in the table (after `Kafka:ConsumerMaxAttempts`, or malformed), what does not, and how it differs from `payment_dead_letters`;
  - the columns;
  - a "List" SQL query, newest first, plus a filter by topic;
  - a "Replay" procedure. Run a `psql` query to fetch the payload and key, pipe them into `docker exec -i ecommerce-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic <Topic> --property parse.key=true --property key.separator='|'`, then optionally `DELETE` the row. Point out that the payload must be a single line, which is true for the JSON Orders produces.
  - why replay is safe (idempotency per handler: US-01 order id, US-04 refund state, US-05 marker);
  - that replaying a malformed message only stores it again, so fix the producer instead.

  Link the document from `README.md` and from `docs/dead-letter-retry.md`, the outbound document, which gets a one-line "see also".
- [ ] T014 [US3] Add `Replayed_Dead_Letter_Is_Processed_Like_A_New_Message` to `tests/Ecommerce.Payments.Component.Tests/MalformedMessageDeadLetterFlowTests.cs`.
  - Insert a `consumed_dead_letters` row whose `Payload` is a **valid** `OrderPlaced`, as if it had been stored during an outage.
  - Re-produce `Payload` with `MessageKey` to `Topic`, exactly as the documented procedure does.
  - Assert one `Processed` payment for the order.
  - Replay a second time and assert there is still one payment.

**Checkpoint**: operators can recover every stored message once its cause is fixed.

---

## Phase 5: Polish & Cross-Cutting Concerns

- [ ] T015 [P] Write ADR **(new)** `docs/adr/0005-inbound-dead-letter.md`, from `docs/adr/template.md`.
  - **Context**: retry in place forever (US-04), and Constitution VI's dead-letter rule.
  - **Decision**: N attempts with backoff, then `consumed_dead_letters` with a unique position and commit only after the store; malformed messages stored with attempts 1; a store failure means no commit; manual replay; a table rather than a topic.
  - **Rejected alternatives**: a dead-letter topic, routing through the Service layer, a persisted attempt count.
  - **Consequences**: ordering within a partition is broken for the skipped message; operators must watch for Error logs.
  - Mention `ecommerce-inventory`'s `consumed_dead_letters`, and add the ADR to `docs/adr/README.md`.
- [ ] T016 [P] Update `README.md`:
  - **Consumer retries paragraph**: add the maximum and the dead-letter step.
  - **Known limitations**: remove "no inbound dead-letter store yet". Add that messages are not replayed automatically and that the attempt count resets on restart.
  - **Background services table**: both consumers dead-letter.
  - **Configuration table**: add `Kafka:ConsumerMaxAttempts`.
  - Link `docs/consumed-dead-letters.md`.
- [ ] T017 [P] Update the project READMEs:
  - `src/Ecommerce.Payments.Consumer/README.md`: `KafkaConsumerBase` behaviour, `InvalidMessageException` and the new setting.
  - `src/Ecommerce.Payments.Infrastructure/README.md`: the table and the store.
- [ ] T018 [P] Update `CLAUDE.md`:
  - new consumers throw `InvalidMessageException` for malformed envelopes and let other exceptions propagate;
  - the base retries, then dead-letters to `consumed_dead_letters`, and commits only after the store;
  - the setting;
  - ADR 0005.
- [ ] T019 Run the full quality gate and `docker build -t ecommerce-payments .`, then mark all tasks `[X]`.
- [ ] T020 Do the manual check from `specs/006-inbound-dead-letter/quickstart.md`, using isolated `us06-manual.*` topics and group as in US-05 T027, and clean up afterwards. Do it **only** if no other local Payments Consumer is running; otherwise skip it and record the reason here.

---

## Dependencies & Execution Order

- **Phase 1** blocks everything.
  - T001 → T002 and T003 → T004.
  - T003 → T005.
  - T006 and T007 are independent.
- **US1** (T008 → T009) depends on Phase 1.
- **US2** (T010, T011 → T012) depends on US1, because T012 reuses the store-then-commit path from T009.
- **US3**: T013 depends only on Phase 1. T014 needs US2's flow-test file to exist, so it comes after T011.
- **Polish**: T015 to T018 can start after US1. T019 and T020 come last.

## Parallel Example: Phase 1

```text
T001 entity + DbContext     T005 KafkaOptions + Program.cs (after T003)     T006 InvalidMessageException
```

## Parallel Example: User Story 2

```text
T010 update existing tests + base invalid-message tests     T011 MalformedMessageDeadLetterFlowTests
```

## Implementation Strategy

1. **Phase 1**: `refactor(US-06): consumed dead-letter store and consumer groundwork`.
2. **US1 (MVP)**: `feat(US-06): dead-letter inbound messages after bounded retries`. This
   closes the Constitution VI gap.
3. **US2**: `feat(US-06): store malformed inbound messages instead of dropping them`.
4. **US3 + Polish**: `docs(US-06): …`, then the quality gate, the Docker build and the manual
   check.

## Notes

- Never commit a dead-lettered message's offset before `StoreAsync` returns.
- Shutdown (`OperationCanceledException` with the stopping token) is never counted and never
  stored.
- If the local Consumer is locking `bin/Debug`, run the quality gate with `-c Release`.
