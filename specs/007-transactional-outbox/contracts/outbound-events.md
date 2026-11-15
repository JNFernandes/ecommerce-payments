# Contract: Outbound Payment Events (US-07)

The payloads, topics and keys are **unchanged** (FR-011):
- [`payments.payment-processed`](../../001-process-payment/contracts/payments.payment-processed.md)
- `payments.payment-failed` (US-02)
- [`payments.payment-refunded`](../../004-refund-on-order-cancelled/contracts/payments.payment-refunded.md)

## Delivery guarantees, as seen by consumers

| Guarantee | Before (US-03) | After (US-07) |
|---|---|---|
| An outcome that is saved is eventually published | Not after a crash between the save and the publish | **Always**: the outcome and its event are saved together |
| Duplicates | Possible after a crash (same `eventId`) | Possible after a crash (same `eventId`); deduplicate by `eventId` |
| Order per payment (processed → refunded) | Not guaranteed when the first event was dead-lettered | **Guaranteed**: recording order per payment id (= Kafka key) |
| Latency when healthy | Immediate | Under 1 s at p95, at most 2 s |
| `eventId` stability | Fixed once serialised | Fixed when recorded; identical on every retry |

## Operator-facing contract: the `payment_outbox` table

See [data-model.md](../data-model.md). It is queryable for support:
- what was recorded, and when;
- what was published, and when;
- what is pending or exhausted, with its retry count and last error.

An `Exhausted` entry can be re-queued as described in `docs/outbox.md`.
