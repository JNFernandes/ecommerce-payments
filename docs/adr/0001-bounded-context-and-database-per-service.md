# 0001. Payments bounded context with its own database

- **Status**: Accepted
- **Date**: 2026-11-12

## Context

The ecommerce system is split into services (Orders, Payments, Inventory, Notifications) that
are developed and deployed independently. Payments needs a little order data to charge a
customer, but the order itself (lines, addresses, lifecycle) belongs to the Orders service.
Sharing an `Order` model or shared tables would couple the schemas and release cycles of both
services and blur who owns which invariant.

## Decision

We will treat Payments as its own bounded context:

- Payments has **no `Order` model**. From `OrderPlaced` it keeps only `orderId` (the event's
  `aggregateId`), `customerId` and the amount (`totalAmount`), stored on its own `Payment`
  aggregate.
- Payments owns **its own PostgreSQL database** (`payments`, provided locally by
  `ecommerce-infra` on `localhost:5434`), with schema changes managed by EF Core migrations in
  this repository.
- There are **no shared tables** with any other service and no cross-service database reads.
  All communication with other services goes through Kafka events.

## Consequences

- Payments can evolve its schema and deploy independently of Orders.
- The order data Payments holds is a snapshot taken from the event; it is never refreshed from
  Orders. Anything else Payments needs about an order must be added to the event contract.
- Consistency between Orders and Payments is eventual and reached through the saga
  ([0002](0002-saga-by-choreography.md)), not through database transactions.
- Reporting that needs order and payment data together must combine them outside both services
  (or via events), never by reading the other service's database.
