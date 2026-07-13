# Research: Handle Payment Failure

## 1. Where does the amount-threshold decision live?

**Decision**: A new `Payment.Evaluate(decimal maxAmountThreshold)` Domain method owns the
decision entirely. It compares `Amount` to the threshold and internally calls either the
existing `Process()` or the new `Fail(reason)`, returning the resulting event as the shared
`PaymentDomainEvent` base type. `ProcessPaymentHandler` calls `Evaluate()` instead of `Process()`
and routes the *result* by its concrete type (`PaymentProcessed` vs `PaymentFailed`) to the
matching publisher method — it never inspects `Amount` itself.

**Rationale**: Constitution Principle I is explicit: "A Service method that branches on payment
amounts, statuses, or business conditions is a violation." The comparison itself must be inside
Domain. Routing an *already-decided* domain event to the matching Kafka topic by C# type
(`switch` on `PaymentProcessed`/`PaymentFailed`) is ordinary orchestration on a result, not a
business rule — the same category of thing `ProcessPaymentHandler` already does when it checks
`ExistsByOrderIdAsync`'s boolean result.

**Alternatives considered**:
- *Put the `if (amount > threshold)` check directly in `ProcessPaymentHandler`*: Rejected —
  directly contradicts the constitution's explicit example of a violation.
- *Change `Payment.Process()`'s signature to `Process(decimal maxAmountThreshold)` and have it
  internally redirect to a failed state*: Rejected — `Process()` is already shipped, tested (21
  passing unit tests), and used by `ProcessPaymentHandler`; its current meaning is "definitely
  transition to Processed." Overloading it to sometimes silently produce a `Failed` outcome
  instead would be a confusing, breaking change to an established contract. A new, explicitly-
  named `Evaluate()` method is additive and leaves `Process()`'s meaning intact for any caller
  that has already decided success (there is none today, but this keeps the two concerns
  separable for testing: `Process()`/`Fail()` are pure state-transition primitives, `Evaluate()`
  is the one place that applies the business rule).
- *A separate Domain "policy" class instead of a `Payment` method*: Considered, but the
  aggregate itself is the natural owner of "what happens when this payment is evaluated" since
  the decision immediately mutates the aggregate's own state; a separate class would just be an
  indirection with no separation-of-concerns benefit at this scale.

## 2. Where does the threshold *value* come from?

**Decision**: A new `PaymentPolicyOptions.MaxAmountThreshold` (`decimal`), bound from
configuration (`PaymentPolicy:MaxAmountThreshold` in `appsettings.json`), read by
`ProcessPaymentHandler` and passed into `Payment.Evaluate()` as a plain parameter. Domain itself
never touches `IConfiguration`/`IOptions` — it only ever sees a `decimal`.

**Rationale**: Spec explicitly says the threshold "is a business-configurable value, not a fixed
constant." Configuration binding is a Service/host-layer concern (same pattern already used for
`KafkaOptions`); Domain stays "plain C#" per Constitution Principle I ("MUST be plain C# — no
dependency on EF Core, Confluent.Kafka, or `.NET Worker` types" — extended here to mean no
configuration-provider dependency either, consistent with the spirit of that rule).

**Alternatives considered**:
- *Hardcode the threshold as a Domain constant*: Rejected — spec explicitly rules this out.
- *Read configuration inside `Payment.Evaluate()` directly*: Rejected — would give Domain a
  framework dependency, breaking "Domain tests MUST be able to run with zero infrastructure."

## 3. Does the failure path get its own retry/idempotency/dead-letter mechanics?

**Decision**: No new mechanics — the failure path reuses everything US-01 already built.
`ExistsByOrderIdAsync` (wrapped in the existing Polly retry pipeline) already gates on "does
*any* Payment row exist for this order," regardless of status, so it already prevents duplicate
failure records for free. `SaveAsync` already persists whatever state the in-memory `Payment` is
in when called — it doesn't need to know whether that state is `Processed` or `Failed`. The
existing dead-letter fallback (`PaymentEventPublisher` catching a publish exception and writing
to `payment_dead_letters`) is extended to the new `PublishFailedAsync` method using the identical
pattern.

**Rationale**: Spec's own Assumptions section states this reuse explicitly ("retry-on-technical-
failure and duplicate-detection mechanisms already established... are reused for the failure
path"). Building parallel infrastructure would violate DRY for no benefit — the failure and
success paths differ only in *which* domain event and *which* topic, not in *how* persistence or
delivery failure is handled.

**Alternatives considered**: None seriously considered — duplicating US-01's retry/dead-letter
machinery for a second, near-identical code path would be a straightforward maintenance
liability with no offsetting benefit.

## 4. Why doesn't a business failure get logged as an error?

**Decision**: No code change is needed beyond *not adding* an error log call on the `Fail()`
branch. `ProcessPaymentHandler`'s existing `try`/`catch` only wraps the DB calls and only logs
via `_logger.LogError` when an *exception* is caught (a technical failure). A business failure
(`Evaluate()` returning `PaymentFailed` instead of `PaymentProcessed`) is a normal return value,
not an exception — it flows through the exact same "save, then publish" code path as a success,
just carrying a different event type. Nothing in that path is error-level by construction.

**Rationale**: Satisfies spec FR-008 ("MUST NOT be logged, alerted, or otherwise surfaced as a
system error") by design rather than by a suppression rule that could be forgotten later. An
optional `_logger.LogInformation` noting the recorded failure (payment id, reason) is reasonable
for observability and does not violate FR-008 since it's explicitly not error/warning severity.

**Alternatives considered**:
- *Throw a custom `PaymentFailedException` and catch it specially*: Rejected — using exceptions
  for expected, non-error control flow is an anti-pattern, and it would require extra code to
  ensure the catch site never accidentally logs it as an error (fragile, easy to regress).
