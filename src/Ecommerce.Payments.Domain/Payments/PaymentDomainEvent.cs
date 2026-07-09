namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Base type for in-process domain events raised by the <see cref="Payment"/> aggregate.
/// These are not yet Kafka messages — the Service layer maps them to integration events
/// after a confirmed database save (Constitution Principle II).
/// </summary>
public abstract class PaymentDomainEvent
{
    protected PaymentDomainEvent(Guid paymentId, Guid orderId)
    {
        PaymentId = paymentId;
        OrderId = orderId;
    }

    /// <summary>The <see cref="Payment"/> aggregate this event describes.</summary>
    public Guid PaymentId { get; }

    /// <summary>The order the payment belongs to.</summary>
    public Guid OrderId { get; }
}
