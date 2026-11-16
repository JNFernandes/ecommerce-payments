# Implementation Plan: Data Retention for Outbox, Dead Letters and Cancellation Markers

**Branch**: `feature/US-09-data-retention` | **Date**: 2026-11-16 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/009-data-retention/spec.md`

## Summary

A new hosted `RetentionWorker` runs `IRetentionPurger` once shortly after startup and then every
hour. The purger deletes, by age:

- published outbox entries older than 30 days;
- inbound dead letters older than 90 days;
- cancellation markers older than 180 days.

Each period is configurable, and `0` keeps that store forever. `Pending` and `Exhausted` outbox
entries are never touched. Rows are deleted in batches of 1 000, using
`DELETE … WHERE key IN (SELECT … LIMIT n FOR UPDATE SKIP LOCKED)`, so locks stay short and
several instances never conflict. Each run logs one count line. Migration `AddRetentionIndexes`
adds three age indexes. No business data is purged, and no contract changes.

## Technical Context

- **Language/Version**: C# / .NET 10
- **Primary Dependencies**: EF Core 10.0.4 + Npgsql 10.0.2. No new packages.
- **Storage**: PostgreSQL. Three new indexes; no table or column changes.
- **Testing**: xUnit, `FakeTimeProvider`, Testcontainers (PostgreSQL).
- **Target Platform**: .NET Worker Service.
- **Project Type**: Event-driven background worker with no HTTP surface.
- **Performance Goals**:
  - Each batch is one indexed delete of at most 1 000 rows, run in its own transaction.
  - An idle run costs three index lookups per hour.
- **Constraints**:
  - Never purge `Pending` or `Exhausted` outbox entries, or business data.
  - Never block startup or payment processing.
  - Safe on several instances.
- **Scale/Scope**: one worker, one purger, an options class, one migration, tests and docs.

There are no open NEEDS CLARIFICATION items; every decision is in [research.md](research.md).

## Constitution Check

*GATE: must pass before Phase 0 research. Re-checked after Phase 1 design.*

Checked against v1.4.0.

| Principle | Check | Status |
|---|---|---|
| I. Layering | Purging is housekeeping of infrastructure tables, kept in `Infrastructure/Retention`. No business rule is involved, so Service and Domain are untouched. This is the same placement as the relay (US-07) and the dead-letter store (US-06). | ✅ |
| II. Write flow and idempotency | Unchanged. Purging deletes only `Published` outbox entries, so it never removes an event before it is published. | ✅ |
| III. Type safety | Typed options and `PurgeResult`. SQL parameters are used for the cutoff and batch size. | ✅ |
| IV/V. Tests | Unit tests for the options and the worker. Integration tests on a real database for every store, for the backlog, for concurrency, and for ordering safety. No Kafka is involved, so no component test is needed. | ✅ |
| VI. Data retention | Each store has its own period, and `0` keeps it forever. `Pending` and `Exhausted` are never purged, and ordering is preserved (R3). Deletes are batched, use `SKIP LOCKED`, and the worker yields first. Each run logs its counts, and failures are retried on the next run. The marker default is long and documented. Business data is never purged. | ✅ |
| VI. Outbox relay ordering | The claim ignores rows that no longer exist, and an integration test proves purging does not block later entries. | ✅ |
| VI. Startup | The worker calls `Task.Yield` before any I/O. | ✅ |
| VII. Branching | `feature/US-09-data-retention`, branched from an up-to-date `main`. Commits use `feat(US-09)`, `test(US-09)` and `docs(US-09)`. | ✅ |
| VIII. Quality gate | Build, then format, then test, after every task. | ✅ |
| Docs | ADR 0007 and a new `docs/retention.md`. Updates to the README (background services, configuration, limitations), `docs/outbox.md`, `docs/consumed-dead-letters.md`, the Infrastructure and Consumer READMEs, and CLAUDE.md. | ✅ |

**Post-design re-check**: unchanged, all ✅.

## Design

```
RetentionWorker (hosted):  Task.Yield → loop { scope → IRetentionPurger.PurgeAsync → log counts } every IntervalMs
RetentionPurger.PurgeAsync:
  now = TimeProvider.GetUtcNow()
  for store in [outbox(Published, PublishedAt), deadLetters(FailedAt), markers(RecordedAt)] where days > 0:
     cutoff = now − days
     do { n = DELETE … WHERE key IN (SELECT key … WHERE expired(cutoff) ORDER BY time LIMIT batch FOR UPDATE SKIP LOCKED) }
     while n == batch
  return PurgeResult(counts)
```

### Components

| Component | Project / path | Kind |
|---|---|---|
| `RetentionOptions`, `PurgeResult`, `IRetentionPurger`, `RetentionPurger`, `RetentionWorker`, `RetentionServiceCollectionExtensions` | Infrastructure `Retention/` | new |
| `PaymentsDbContext` (three indexes) | Infrastructure `Persistence/` | extend |
| Migration `AddRetentionIndexes` | Infrastructure `Persistence/Migrations/` | new |
| DI, options validation, hosted service, `Retention` section | Consumer `Program.cs`, `appsettings.json` | extend |
| ADR 0007, `docs/retention.md`, README, `docs/outbox.md`, `docs/consumed-dead-letters.md`, project READMEs, CLAUDE.md | docs | new / extend |

## Project Structure

```text
specs/009-data-retention/  spec.md plan.md research.md data-model.md quickstart.md checklists/ tasks.md
src/Ecommerce.Payments.Infrastructure/Retention/*.cs                                   (new)
src/Ecommerce.Payments.Infrastructure/Persistence/PaymentsDbContext.cs                 (extend)
src/Ecommerce.Payments.Infrastructure/Persistence/Migrations/<ts>_AddRetentionIndexes.cs (new)
src/Ecommerce.Payments.Consumer/Program.cs  appsettings.json                           (extend)
tests/Ecommerce.Payments.Infrastructure.Tests/Retention/RetentionOptionsTests.cs  RetentionWorkerTests.cs (new)
tests/Ecommerce.Payments.Integration.Tests/RetentionPurgerTests.cs                     (new)
docs/adr/0007-data-retention.md  docs/retention.md                                     (new)
```

**Structure Decision**: Keep the existing projects; add none.

## Risks

- **Purging a marker too early.** A very late `OrderPlaced` would then be charged. The default
  is 180 days, and the trade-off is documented in `docs/retention.md` and ADR 0007.
- **A large first purge on an old database.** It is bounded per batch, and each batch commits on
  its own, so it can be interrupted safely. It may take several batches the first time.
- **Purging dead letters before anyone replays them.** The default is 90 days, and the docs
  advise exporting or replaying rows before they expire.

## Complexity Tracking

None.
