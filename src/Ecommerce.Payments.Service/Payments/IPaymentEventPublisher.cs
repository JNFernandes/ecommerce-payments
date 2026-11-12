using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Publishes payment domain events to Kafka as integration events. Invoked by the Service layer
/// only after the Repository call has returned success (Constitution Principle II).
/// </summary>
public interface IPaymentEventPublisher
{
    /// <summary>
    /// Serializes and publishes a <see cref="PaymentProcessed"/> domain event to
    /// <c>payments.payment-processed</c>.
    /// </summary>
    public Task PublishAsync(PaymentProcessed paymentProcessed, CancellationToken cancellationToken);

    /// <summary>
    /// Serializes and publishes a <see cref="PaymentFailed"/> domain event to
    /// <c>payments.payment-failed</c>.
    /// </summary>
    public Task PublishFailedAsync(PaymentFailed paymentFailed, CancellationToken cancellationToken);

    /// <summary>
    /// Serializes and publishes a <see cref="PaymentRefunded"/> domain event to
    /// <c>payments.payment-refunded</c>, keyed by the payment id. A publish failure is
    /// dead-lettered for the retry worker rather than thrown.
    /// </summary>
    public Task PublishRefundedAsync(PaymentRefunded paymentRefunded, CancellationToken cancellationToken);
}
