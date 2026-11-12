namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Domain event raised by <see cref="Payment.Refund"/> when a payment transitions from
/// <see cref="PaymentStatus.Processed"/> to <see cref="PaymentStatus.Refunded"/>.
/// </summary>
public sealed class PaymentRefunded : PaymentDomainEvent
{
    /// <summary>Creates the event.</summary>
    public PaymentRefunded(Guid paymentId, Guid orderId, decimal amount, string currency, DateTimeOffset refundedAt)
        : base(paymentId, orderId)
    {
        Amount = amount;
        Currency = currency;
        RefundedAt = refundedAt;
    }

    /// <summary>The amount refunded — always the full amount of the payment.</summary>
    public decimal Amount { get; }

    /// <summary>The ISO 4217 currency code of the refund.</summary>
    public string Currency { get; }

    /// <summary>When the payment transitioned to <see cref="PaymentStatus.Refunded"/>.</summary>
    public DateTimeOffset RefundedAt { get; }
}
