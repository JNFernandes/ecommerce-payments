# Consumed dead letters (inbound)

When Payments cannot handle an **inbound** message (`orders.order-placed` or
`orders.order-cancelled`), it stores the message exactly as received in the
`consumed_dead_letters` table. Only after that does it commit the offset, so one bad message
cannot block its partition (Constitution Principle VI).

Design decision: [ADR 0005, Inbound dead-letter](adr/0005-inbound-dead-letter.md).
Feature spec: [`specs/006-inbound-dead-letter`](../specs/006-inbound-dead-letter/spec.md) (US-06).

This is **not** the outbound table. `payment_outbox` holds every event Payments *publishes*,
and the outbox relay delivers and retries those automatically ([outbox.md](outbox.md)).
Nothing replays `consumed_dead_letters` automatically.

## What lands here

| Case | `Attempts` | Log |
|---|---|---|
| Malformed envelope: unreadable JSON, or a missing or invalid required field | `1` | Warning "Malformed message … stored in consumed_dead_letters and committed." |
| Handling failed `Kafka:ConsumerMaxAttempts` times in a row (default 5, about 15 s of backoff) | `>= max` | Error "Dead-lettered message … after N attempts …" |

**What does not land here:**
- A message that succeeds within the maximum number of attempts.
- A message whose store failed. That message is not committed and keeps being retried; look for
  the Error "Could not dead-letter message …". This is normal while the database is down.
- Shutdown.

The attempt count lives in memory, so after a restart the count starts again.

## Retention

Rows are purged after `Retention:ConsumedDeadLettersDays` (default 90 days; `0` keeps them
forever). Replay or export the rows you still need before then. See [retention.md](retention.md)
(US-09).

## Columns

| Column | Meaning |
|---|---|
| `Id` | Row id |
| `Topic`, `Partition`, `Offset` | Where the message came from; unique (one row per Kafka position) |
| `MessageKey` | Key as received (may be null) |
| `Payload` | Value as received, even if it is not JSON (may be null) |
| `Error` | `Type: message`, plus ` ---> InnerType: inner message` for the innermost cause |
| `Attempts` | Failed attempts before storing; `1` means malformed |
| `FailedAt` | When it was stored (UTC) |

## List

Connect to the payments database. Against `ecommerce-infra` that is
`docker exec -it ecommerce-postgres-payments-1 psql -U payments_user -d payments`. Then:

```sql
-- Newest first
SELECT "Id", "Topic", "Partition", "Offset", "MessageKey", "Attempts", "FailedAt", "Error"
FROM consumed_dead_letters
ORDER BY "FailedAt" DESC
LIMIT 50;

-- One topic only, including the payload
SELECT "Id", "Offset", "MessageKey", "Payload", "Error"
FROM consumed_dead_letters
WHERE "Topic" = 'orders.order-placed'
ORDER BY "FailedAt" DESC;
```

## Replay

Fix the cause first, for example a database outage or a bug. Then produce the stored payload,
with its key, to its original topic. The service handles it like any new message.

1. Find the row's `Id` with the queries above.
2. Replay it. This example runs against `ecommerce-infra` from Git Bash or WSL; replace `<id>`:

   ```bash
   ID='<id>'
   TOPIC=$(docker exec ecommerce-postgres-payments-1 psql -U payments_user -d payments -At \
     -c "SELECT \"Topic\" FROM consumed_dead_letters WHERE \"Id\" = '$ID'")
   docker exec ecommerce-postgres-payments-1 psql -U payments_user -d payments -At \
     -c "SELECT coalesce(\"MessageKey\", '') || '|' || \"Payload\" FROM consumed_dead_letters WHERE \"Id\" = '$ID'" \
   | docker exec -i ecommerce-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 \
       --topic "$TOPIC" --property parse.key=true --property key.separator='|'
   ```

   - `kafka-console-producer` sends one message per line. JSON produced by Orders is a single
     line. Check that the payload contains no line breaks, and that the key contains no `|`,
     before replaying.
   - Without a key, the message is produced with an empty key.
3. Watch the service log for the normal outcome, for example a payment processed or a refund.
4. Optionally remove the row:
   `DELETE FROM consumed_dead_letters WHERE "Id" = '<id>';`.

### Why replay is safe

Every handler is idempotent, so replaying a message that was in fact already handled changes
nothing:

| Message | Idempotency |
|---|---|
| `OrderPlaced` | At most one payment per order (US-01, unique `order_id`). A marked (cancelled) order is never charged (US-05). |
| `OrderCancelled` | A refund happens only from `Processed`, and the refund's `eventId` is stored (US-04). With no payment, the cancellation marker is unique per order (US-05). |

Replaying a **malformed** message only stores it again, as a new row under its new offset. Fix
the producer instead.
