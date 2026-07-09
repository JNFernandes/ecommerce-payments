namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>payment_dead_letters</c> table. Holds integration events
/// that failed to publish after a successful <c>Payment</c> save (Constitution Principle II) —
/// the write path only; replay/reprocessing of these rows is out of scope for this feature.
/// </summary>
public sealed class PaymentDeadLetterEntity
{
    public Guid Id { get; set; }

    public Guid PaymentId { get; set; }

    public string EventType { get; set; } = string.Empty;

    /// <summary>The serialized integration event that failed to publish.</summary>
    public string Payload { get; set; } = string.Empty;

    public string FailureReason { get; set; } = string.Empty;

    public DateTimeOffset CreatedAt { get; set; }
}
