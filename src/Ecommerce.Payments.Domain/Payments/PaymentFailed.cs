namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Domain event raised by <see cref="Payment.Fail"/> when a payment transitions from
/// <see cref="PaymentStatus.Pending"/> to <see cref="PaymentStatus.Failed"/>.
/// </summary>
public sealed class PaymentFailed : PaymentDomainEvent
{
    public PaymentFailed(Guid paymentId, Guid orderId, string reason, DateTimeOffset failedAt)
        : base(paymentId, orderId)
    {
        Reason = reason;
        FailedAt = failedAt;
    }

    /// <summary>Human-readable business reason the payment could not be completed.</summary>
    public string Reason { get; }

    /// <summary>When the payment transitioned to <see cref="PaymentStatus.Failed"/>.</summary>
    public DateTimeOffset FailedAt { get; }
}
