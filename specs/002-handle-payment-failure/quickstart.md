# Quickstart: Handle Payment Failure

Validation guide for confirming this feature works end-to-end once implemented. Builds directly
on [001-process-payment/quickstart.md](../001-process-payment/quickstart.md) — same
Prerequisites/Setup; only the scenarios below are new. See [data-model.md](./data-model.md) and
[contracts/](./contracts/) for the exact payloads referenced below.

## Setup

Same as US-01, plus one new configuration value to be aware of:
`PaymentPolicy:MaxAmountThreshold` in `appsettings.json` (or the `PaymentPolicy__MaxAmountThreshold`
environment variable) controls the amount above which a payment is recorded as failed rather
than processed. The scenarios below assume a threshold of `10000.00`.

## Scenario 1 — Payment exceeds the threshold (User Story 1)

1. Produce a valid `OrderPlaced` message (see
   [001-process-payment/contracts/orders.order-placed.md](../001-process-payment/contracts/orders.order-placed.md))
   whose `totalAmount` is above the configured threshold (e.g. `15000.00`) to
   `orders.order-placed`.
2. **Expected**: within a few seconds, a `payments` row exists with `status = Failed`,
   `FailureReason` populated (referencing the threshold), and `FailedAt` set; `Amount` matches
   the message's `totalAmount`.
3. **Expected**: a message appears on `payments.payment-failed` matching
   [contracts/payments.payment-failed.md](./contracts/payments.payment-failed.md), with
   `orderId` correlating back to the input message and `reason` describing the threshold breach.
4. **Expected**: no message appears on `payments.payment-processed` for this order.

## Scenario 2 — Duplicate delivery of a failing order (User Story 2)

1. Re-produce the **exact same** `OrderPlaced` message from Scenario 1 to `orders.order-placed`.
2. **Expected**: no second row is created in `payments` for that `order_id` — same idempotency
   guarantee as the success path.
3. **Expected**: no second message is published to `payments.payment-failed`.

## Scenario 3 — Below-threshold order still succeeds (regression check)

1. Produce a valid `OrderPlaced` message with `totalAmount` below the threshold (e.g. `129.99`).
2. **Expected**: identical to US-01 Scenario 1 — `payments` row `status = Processed`,
   `PaymentProcessed` published. Confirms this feature didn't change the existing success path.

## Automated equivalents

- Scenario 1 → Component test (testcontainers Kafka + PostgreSQL), see Test Plan in
  [spec.md](./spec.md).
- Scenario 2 → Component test, redelivery variant.
- Scenario 3 → Already covered by
  [001-process-payment/quickstart.md](../001-process-payment/quickstart.md) Scenario 1's
  automated equivalent; re-run as a regression check, not a new test.
- Technical failure while saving a `Failed` payment (retry, no premature publish) → same
  mechanism as [001-process-payment](../001-process-payment/quickstart.md) Scenario 3; not
  re-tested from scratch here (see research.md #3).
