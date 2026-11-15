using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Records outgoing payment events in the transactional outbox (Constitution v1.3.0,
/// Principle II; US-07, ADR 0006). Call it inside the per-order transaction
/// (<see cref="Orders.IOrderUnitOfWork"/>), after the payment change has been saved and before
/// committing: the entry then becomes durable exactly when the state change does. It never
/// publishes — the outbox relay does that, after the commit.
/// </summary>
public interface IPaymentOutbox
{
    /// <summary>Records a <c>PaymentProcessed</c> event for <c>payments.payment-processed</c>.</summary>
    public Task AddProcessedAsync(PaymentProcessed paymentProcessed, CancellationToken cancellationToken);

    /// <summary>Records a <c>PaymentFailed</c> event for <c>payments.payment-failed</c>.</summary>
    public Task AddFailedAsync(PaymentFailed paymentFailed, CancellationToken cancellationToken);

    /// <summary>Records a <c>PaymentRefunded</c> event for <c>payments.payment-refunded</c>.</summary>
    public Task AddRefundedAsync(PaymentRefunded paymentRefunded, CancellationToken cancellationToken);
}
