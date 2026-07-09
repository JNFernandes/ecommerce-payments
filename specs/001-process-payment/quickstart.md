# Quickstart: Process Payment from Order Placed Event

Validation guide for confirming this feature works end-to-end once implemented. See
[data-model.md](./data-model.md) for entity shapes and [contracts/](./contracts/) for the exact
event payloads referenced below.

## Prerequisites

- .NET 10 SDK
- Docker (for PostgreSQL + Kafka via docker-compose, and for the testcontainers-based test
  suites)
- A Kafka client capable of producing a single JSON message to a topic (e.g., `kcat`/`kafkacat`,
  or `kafka-console-producer` from a Kafka Docker image)

## Setup

```powershell
# From repo root:
docker-compose up -d postgres kafka

# Apply EF Core migrations for the Payments schema
dotnet ef database update --project src/Ecommerce.Payments.Infrastructure

# Run the consumer
dotnet run --project src/Ecommerce.Payments.Consumer
```

**Note**: if a shared `ecommerce-infra` stack (used by sibling services like `ecommerce-orders`)
is already running, its Postgres/Kafka default to the same ports as this repo's own
`docker-compose.yml` and the two will conflict. To see this service interact with the real
`orders` service, point `ConnectionStrings:Payments` / `Kafka:BootstrapServers` (via environment
variables) at the shared stack instead of starting this repo's own containers — see
`ecommerce-orders/.env` for that stack's actual ports/credentials.

## Scenario 1 — Happy path (User Story 1)

1. Produce a single valid `OrderPlaced` message (see
   [contracts/orders.order-placed.md](./contracts/orders.order-placed.md) for the payload shape)
   to `orders.order-placed`.
2. **Expected**: within a few seconds, a `payments` row exists with `status = Processed`,
   `order_id` matching the message's `aggregateId`, `amount` matching `totalAmount`, and
   `currency` set to the fixed platform default (`USD`) — the message itself carries no currency.
3. **Expected**: a message appears on `payments.payment-processed` matching
   [contracts/payments.payment-processed.md](./contracts/payments.payment-processed.md), with
   `orderId` correlating back to the input message.

## Scenario 2 — Duplicate delivery (User Story 2)

1. Re-produce the **exact same** `OrderPlaced` message from Scenario 1 to `orders.order-placed`.
2. **Expected**: no second row is created in `payments` for that `order_id`.
3. **Expected**: no second message is published to `payments.payment-processed`.

## Scenario 3 — Transient save failure (User Story 3)

1. Stop the PostgreSQL container: `docker-compose stop postgres`.
2. Produce a new, valid `OrderPlaced` message.
3. **Expected**: the consumer logs a save failure with full context (order id, error detail) and
   does **not** publish to `payments.payment-processed`.
4. Restart PostgreSQL: `docker-compose start postgres`.
5. **Expected**: without any manual intervention, the message is retried and Scenario 1's
   expected outcome is eventually observed (row saved, event published).

## Scenario 4 — Malformed message (Edge Case)

1. Produce a message to `orders.order-placed` missing a required field (e.g., no `totalAmount`).
2. **Expected**: no `payments` row is created, no publish occurs, and a validation failure is
   logged referencing the malformed message (not a stack trace from Domain code — it must never
   reach Domain).

## Automated equivalents

Each manual scenario above has an automated counterpart to run in CI:

- Scenario 1 → Component test (testcontainers Kafka + PostgreSQL), see Test Plan in
  [spec.md](./spec.md).
- Scenario 2 → Component test, redelivery variant.
- Scenario 3 → Integration test (testcontainers PostgreSQL) simulating a save failure.
- Scenario 4 → Unit test at the Consumer/validation boundary.
