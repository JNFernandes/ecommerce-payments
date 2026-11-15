namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// Delivery state of an <see cref="OutboxEntryEntity"/> (Constitution v1.3.0, Principle VI
/// "Outbox relay"; ADR 0006). Stored as a string in <c>payment_outbox."Status"</c>.
/// </summary>
public enum OutboxStatus
{
    /// <summary>Waiting for the relay; eligible once <c>NextAttemptAt</c> has passed and every earlier entry for the payment is published.</summary>
    Pending,

    /// <summary>Published successfully. Terminal — never published again by the relay.</summary>
    Published,

    /// <summary>Gave up after the configured maximum retries. Needs operator attention; blocks later entries for the same payment until re-queued.</summary>
    Exhausted
}
