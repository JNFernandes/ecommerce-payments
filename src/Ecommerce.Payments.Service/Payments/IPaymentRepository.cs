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
}
