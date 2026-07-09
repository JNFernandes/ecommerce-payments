namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Domain event raised by <see cref="Payment.Process"/> when a payment successfully transitions
/// from <see cref="PaymentStatus.Pending"/> to <see cref="PaymentStatus.Processed"/>.
/// </summary>
public sealed class PaymentProcessed : PaymentDomainEvent
{
    public PaymentProcessed(Guid paymentId, Guid orderId, decimal amount, string currency, DateTimeOffset processedAt)
        : base(paymentId, orderId)
    {
        Amount = amount;
        Currency = currency;
        ProcessedAt = processedAt;
    }

    /// <summary>The amount that was charged.</summary>
    public decimal Amount { get; }

    /// <summary>The ISO 4217 currency code of the charge.</summary>
    public string Currency { get; }

    /// <summary>When the payment transitioned to <see cref="PaymentStatus.Processed"/>.</summary>
    public DateTimeOffset ProcessedAt { get; }
}
