namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// Retry state of a <see cref="PaymentDeadLetterEntity"/> (Constitution Principle VI
/// "Dead-letter retry", ADR 0003). Stored as a string in <c>payment_dead_letters."Status"</c>.
/// </summary>
public enum DeadLetterStatus
{
    /// <summary>Waiting for the retry worker; eligible once <c>NextRetryAt</c> has passed.</summary>
    PendingRetry,

    /// <summary>Republished successfully. Terminal — never retried again.</summary>
    Resolved,

    /// <summary>Gave up after the configured maximum retries. Terminal — needs operator attention.</summary>
    Exhausted
}
