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

    /// <summary>
    /// Backoff after the <paramref name="retryCount"/>-th failed retry:
    /// <c>min(BaseDelayMs × 2^(retryCount − 1), MaxDelayMs)</c>. With the defaults:
    /// 5 s, 10 s, 20 s, … capped at 10 minutes. Never overflows for large counts.
    /// </summary>
    /// <param name="retryCount">Number of failed retries so far (1 or more).</param>
    /// <param name="options">Retry settings.</param>
    /// <returns>Delay until the next retry.</returns>
    public static TimeSpan ComputeDelay(int retryCount, DeadLetterRetryOptions options)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retryCount, 1);

        var exponent = Math.Min(retryCount - 1, 62);
        var delayMs = Math.Min((double)options.BaseDelayMs * Math.Pow(2, exponent), options.MaxDelayMs);
        return TimeSpan.FromMilliseconds(delayMs);
    }

    /// <summary>
    /// Records a failed retry: increments <see cref="PaymentDeadLetterEntity.RetryCount"/>, stores
    /// the error and attempt time, and either schedules the next retry with
    /// <see cref="ComputeDelay"/> or, once <see cref="DeadLetterRetryOptions.MaxRetries"/> is
    /// reached, marks the row <see cref="DeadLetterStatus.Exhausted"/> so it is never retried again.
    /// </summary>
    /// <param name="row">The claimed dead-letter row.</param>
    /// <param name="error">Error message of the failed publish.</param>
    /// <param name="now">Current time.</param>
    /// <param name="options">Retry settings.</param>
    /// <returns><see langword="true"/> when the row became <see cref="DeadLetterStatus.Exhausted"/>.</returns>
    public static bool ApplyFailure(
        PaymentDeadLetterEntity row,
        string error,
        DateTimeOffset now,
        DeadLetterRetryOptions options)
    {
        row.RetryCount++;
        row.LastError = error;
        row.LastAttemptAt = now;

        if (row.RetryCount >= options.MaxRetries)
        {
            row.Status = DeadLetterStatus.Exhausted;
            return true;
        }

        row.NextRetryAt = now + ComputeDelay(row.RetryCount, options);
        return false;
    }
}
