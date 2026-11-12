using Ecommerce.Payments.Infrastructure.Persistence.Entities;

namespace Ecommerce.Payments.Infrastructure.DeadLetters;

/// <summary>
/// Pure retry decisions for <see cref="PaymentDeadLetterEntity"/> rows (no I/O, no clock of its
/// own — the caller passes <c>now</c> from <see cref="TimeProvider"/>). Only bookkeeping fields
/// are changed; the payload, topic, key and event type are never touched, so a republished
/// event keeps its original <c>eventId</c> (ADR 0003).
/// </summary>
public static class DeadLetterRetryPolicy
{
    /// <summary>Marks a row as successfully republished.</summary>
    /// <param name="row">The claimed dead-letter row.</param>
    /// <param name="now">Current time.</param>
    public static void ApplySuccess(PaymentDeadLetterEntity row, DateTimeOffset now)
    {
        row.Status = DeadLetterStatus.Resolved;
        row.ResolvedAt = now;
        row.LastAttemptAt = now;
    }
}
