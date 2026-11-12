using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Persists <see cref="Payment"/> aggregates to PostgreSQL. Enforces idempotency at the data
/// layer via a unique constraint on <c>order_id</c> (Constitution Principle II).
/// </summary>
public interface IPaymentRepository
{
    /// <summary>Durably saves a new or updated <see cref="Payment"/> aggregate.</summary>
    public Task SaveAsync(Payment payment, CancellationToken cancellationToken);

    /// <summary>
    /// Returns <see langword="true"/> if a <see cref="Payment"/> already exists for the given
    /// <paramref name="orderId"/> — the idempotency check the Service layer runs before invoking
    /// Domain (Constitution Principle II).
    /// </summary>
    public Task<bool> ExistsByOrderIdAsync(Guid orderId, CancellationToken cancellationToken);

    /// <summary>
    /// Loads the <see cref="Payment"/> for <paramref name="orderId"/>, or returns
    /// <see langword="null"/> when the order has no payment (a lookup result, not an error).
    /// </summary>
    public Task<Payment?> GetByOrderIdAsync(Guid orderId, CancellationToken cancellationToken);

    /// <summary>
    /// Durably saves a refund already applied to <paramref name="payment"/> by the Domain
    /// (<see cref="Payment.Refund"/>). The update is conditional on the stored payment still being
    /// <see cref="PaymentStatus.Processed"/>, so two concurrent deliveries cannot both refund it.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the refund was saved; <see langword="false"/> when the stored
    /// payment was no longer Processed (refunded concurrently) and nothing was written.
    /// </returns>
    public Task<bool> SaveRefundAsync(Payment payment, CancellationToken cancellationToken);
}
