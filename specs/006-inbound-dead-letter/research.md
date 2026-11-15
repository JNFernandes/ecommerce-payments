# Research: Inbound Dead-Letter (US-06)

All decisions below are made; nothing is left as NEEDS CLARIFICATION.

## R1. Where the dead-letter routing lives

**Decision**: In the shared consumer loop, `KafkaConsumerBase`, in the Consumer layer. It writes
through a small Infrastructure port, `IConsumedDeadLetterStore`, resolved from a fresh DI scope
for each store. The store does not go through the Service layer.

**Rationale**:
- Constitution I already assigns this job to the Consumer: unhandled exceptions are "logged and
  routed to retry/dead-letter", and the Consumer "owns offset commit strategy".
- The record is a transport artefact (topic, partition, offset, raw bytes), not business state.
  No `Payment` or marker changes, and nothing is published.
- Both consumers get the behaviour for free (FR-009).
- The Consumer project already references Infrastructure, for example `KafkaOptions`.

**Alternatives**:
- *A Service-layer handler*: the Service would then have to understand Kafka positions, which
  leaks transport concerns upward.
- *A dead-letter Kafka topic*: needs topic provisioning in `ecommerce-infra` (auto-create is
  off) and more tooling to inspect it. The constitution allows either a table or a topic; a
  table matches Inventory and the existing outbound table.

## R2. Telling "malformed" apart from "failed"

**Decision**: Add `InvalidMessageException(string reason)` in the Consumer project. When
`TryParse` fails, `HandleMessageAsync` throws it, where today it logs and returns `true`. The
base catches it and dead-letters immediately with attempts = 1, logged at Warning, without
backoff or retry (FR-006). Every other exception counts toward the maximum.

**Rationale**:
- The base needs the message's position and key, which only it holds, so the subclass cannot
  store the message itself.
- An exception keeps `HandleMessageAsync`'s `Task<bool>` signature, which about 30 existing test
  call sites rely on.
- It mirrors Inventory's non-retryable `InvalidMessageException`.

**Alternatives**: returning a result enum instead of `bool`. That is cleaner in isolation but
touches every existing call site for no behavioural gain.

**Impact**: two existing tests expect `HandleMessageAsync` to return `true` for malformed input.
They will expect `InvalidMessageException` instead:
- `OrderCancelledToRefundTests.HandleMessageAsync_MalformedMessage_IsCommitted` (theory, two
  cases);
- `CancellationBeforePaymentTests.Malformed_OrderCancelled_Records_No_Marker`.

No existing test sends a malformed `OrderPlaced`.

**Null value**: the base passes `Value ?? string.Empty` to `HandleMessageAsync(string)`, so a
null (tombstone) value fails `TryParse` and is reported as an `InvalidMessageException`. The
override signature stays `string`.

## R3. Counting attempts

**Decision**: Count consecutive failures in memory in the consume loop, together with the
`TopicPartitionOffset` of the failing message. Reset the count when a message succeeds, when it
is stored, or when the consumed message is a different one (for example after a rebalance moved
the partition). When `attempts >= ConsumerMaxAttempts`, try to store the message.

**Rationale**: in-place retry means consecutive failures are, in practice, attempts on the same
message. Tracking the offset guards against a rebalance. A count that persisted across restarts
would buy only fewer extra retries, and the spec accepts those.

## R4. When the store itself fails

**Decision**: If `StoreAsync` throws, log an Error ("could not dead-letter; will retry"), do not
commit, seek back, and wait the capped backoff. The next round runs normal handling first. If
that fails again, the count is still at or above the maximum, so the store is tried again
(FR-003).

**Rationale**: the most likely reason a message keeps failing is that the database is down, and
then the store fails too. Retrying handling first means that once the database is back, the
message is processed rather than parked.

## R5. Idempotent store

**Decision**: Use the table `consumed_dead_letters` with a unique index on
`(Topic, Partition, Offset)`. The insert is
`INSERT … ON CONFLICT ("Topic","Partition","Offset") DO NOTHING`, run with `ExecuteSqlAsync`
(parameterised). It writes no EF tracking, so a crash between the store and the commit leads to a
harmless second insert (FR-004).

## R6. Message key and value

**Decision**: Change the base's consumer from `IConsumer<Ignore, string>` to
`IConsumer<string?, string?>` (UTF-8 deserialisers) so the key is available to store. Store the
key and the value as text, both nullable. A null value, for example a tombstone, is malformed.

**Rationale**: Orders keys messages with the order id as a UTF-8 string, so text is enough. A
non-UTF-8 key would come out garbled but still identifiable by its partition and offset.

## R7. Error text

**Decision**: Store `"{Type}: {Message}"` of the caught exception. When the innermost exception
differs, append `" ---> {InnerType}: {InnerMessage}"` for it. For example `DbUpdateException`
wraps `PostgresException`, and the inner one says what actually failed. Do not store the stack
trace, which stays in the log.

## R8. Settings

**Decision**: Add `Kafka:ConsumerMaxAttempts`, default 5. Validate at startup that it is > 0,
and validate the existing retry delays too, using `AddOptions<KafkaOptions>().Validate(...)
.ValidateOnStart()`. With the default backoff, the time before a message is parked is about
1 + 2 + 4 + 8 = 15 s, which meets SC-001.

## R9. Replay

**Decision**: Replay is a documented manual procedure in `docs/consumed-dead-letters.md`:
1. Select the row.
2. Produce its `Payload` with its `MessageKey` to its `Topic` using `kafka-console-producer`, with
   `parse.key=true` and a separator.
3. Optionally delete the row.

There is no code. Idempotency (US-01, US-04, US-05) makes a replay of an already-handled
message a no-op.

## R10. Documentation and decision record

**Decision**:
- Add **ADR 0005**, "Inbound dead-letter after bounded consumer retries". It records that Payments
  now routes inbound messages to a dead-letter store, closing US-04's open item.
- Update `README.md`: known limitations, the consumer-retries paragraph, the table list and the
  configuration.
- Update the Consumer and Infrastructure READMEs, `CLAUDE.md`, and `docs/dead-letter-retry.md`,
  which gets a pointer explaining that it covers outbound dead letters only.
