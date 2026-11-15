# Architecture Decision Records

Architectural decisions for `ecommerce-payments` are recorded here, as the project constitution
requires (Documentation Standards).

## Conventions

- One decision per file, named `NNNN-short-title.md` with a four-digit sequential number
  (`0001-...`, `0002-...`).
- Numbers are never reused or renumbered. To reverse a decision, write a new ADR and set the old
  one's status to `Superseded by NNNN`.
- Start from [`template.md`](template.md) (Context / Decision / Consequences).
- Add every new ADR to the index below in the same PR.

## Index

| # | Title | Status |
|---|---|---|
| [0001](0001-bounded-context-and-database-per-service.md) | Payments bounded context with its own database | Accepted |
| [0002](0002-saga-by-choreography.md) | Order/payment saga by choreography | Accepted |
| [0003](0003-dead-letter-retry.md) | Dead-letter retry worker with SKIP LOCKED claiming and exponential backoff | Superseded by 0006 |
| [0004](0004-per-order-serialisation.md) | Per-order serialisation of placement and cancellation (cancellation markers + advisory lock) | Accepted |
| [0005](0005-inbound-dead-letter.md) | Inbound dead-letter after bounded consumer retries | Accepted |
| [0006](0006-transactional-outbox.md) | Transactional outbox for payment events | Accepted |
