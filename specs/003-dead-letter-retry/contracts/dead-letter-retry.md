# Contract: Dead-Letter Retry

This feature adds **no new topics and no new event types**. It defines (1) the republish
guarantee towards existing consumers and (2) the operator-facing configuration and table
contract.

## 1. Republished events (towards downstream consumers)

| Topic | Event | Contract |
|---|---|---|
| `payments.payment-processed` | `PaymentProcessed` | Unchanged — see `specs/001-process-payment/contracts/payments.payment-processed.md`. |
| `payments.payment-failed` | `PaymentFailed` | Unchanged — see `specs/002-handle-payment-failure/contracts/payments.payment-failed.md`. |

Guarantees for a republished message:

- **Value**: byte-for-byte the payload stored when the original publish failed. `eventId`,
  `occurredAt`, `aggregateId` (payment id), `orderId` and all other fields are identical.
- **Key**: the original key (payment id as string).
- **Topic**: the original topic.
- **Delivery**: at-least-once. The same `eventId` MAY arrive more than once (e.g. the original
  publish actually reached the broker before timing out, or the row update failed after a
  successful republish). Consumers MUST deduplicate by `eventId`.
- **Ordering**: not guaranteed relative to other events.

## 2. Configuration (`appsettings.json`, section `DeadLetterRetry`)

```json
"DeadLetterRetry": {
  "IntervalMs": 30000,
  "BatchSize": 20,
  "BaseDelayMs": 5000,
  "MaxDelayMs": 600000,
  "MaxRetries": 8
}
```

Overridable via environment variables, e.g. `DeadLetterRetry__IntervalMs=5000`. Invalid values
(≤ 0, or `MaxDelayMs < BaseDelayMs`) fail at startup.

## 3. Table contract (`payment_dead_letters`)

Operators may inspect the table and insert test rows (see `docs/dead-letter-retry.md`). Columns
and states: [data-model.md](../data-model.md). A row inserted with `Status = 'PendingRetry'` and
`NextRetryAt <= now()` is picked up by the next worker cycle.

## 4. Logs

| Event | Level | Context |
|---|---|---|
| Republish succeeded | Information | dead-letter id, payment id, event type, topic, attempt |
| Republish failed, rescheduled | Warning | + retry count, next retry time, error |
| Row exhausted | Error | dead-letter id, payment id, event type, topic, retry count, last error |
| Cycle failed (DB/unexpected) | Error | exception; worker continues next interval |
