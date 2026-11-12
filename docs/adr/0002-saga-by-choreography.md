# 0002. Order/payment saga by choreography

- **Status**: Accepted
- **Date**: 2026-11-12

## Context

Placing an order spans two services with separate databases
([0001](0001-bounded-context-and-database-per-service.md)): Orders and Payments. A distributed
transaction across them is not an option, so the outcome has to be reached through a saga of
local transactions linked by events. The flow is short and linear, with one decision point (did
the payment succeed?) and one compensation (refund when an order is cancelled).

## Decision

We will implement the saga by **choreography**, with no central orchestrator. Each service
reacts to the other's events:

1. Orders publishes `OrderPlaced` (`orders.order-placed`).
2. Payments creates a `Payment`, saves it, and publishes `PaymentProcessed`
   (`payments.payment-processed`) or `PaymentFailed` (`payments.payment-failed`).
3. Orders confirms or cancels the order based on that outcome.
4. When an order is cancelled, Orders publishes `OrderCancelled` (`orders.order-cancelled`);
   Payments refunds a `Processed` payment and publishes `PaymentRefunded`
   (`payments.payment-refunded`). Refund requests for `Failed`, already `Refunded` or unknown
   payments are logged and ignored.

Every payment event carries `aggregateId` = the payment id and a separate `orderId`; Orders
correlates on `orderId`.

This mirrors the matching decision in the `ecommerce-orders` repository:
`docs/adr/0003-saga-by-choreography.md` ("Order/payment saga by choreography").

## Consequences

- No coordinator service to build, deploy or keep available; each service owns its own step and
  compensation.
- The saga's flow is implicit in the event contracts, so those contracts (topics, field
  semantics such as `aggregateId` vs `orderId`) must be documented and kept in sync across both
  repositories.
- Every handler must be idempotent and tolerate redelivery and out-of-order events (e.g. an
  `OrderCancelled` arriving for a payment that failed or does not exist).
- Adding more participants (e.g. Inventory reservation) increases the number of event
  interactions to reason about; if the flow grows complex, orchestration should be reconsidered
  in a new ADR.
