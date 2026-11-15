using Ecommerce.Payments.Domain.Orders;

namespace Ecommerce.Payments.Service.Orders;

/// <summary>
/// Persists <see cref="CancellationMarker"/>s. At most one marker exists per order, enforced by
/// the primary key on the order id (Constitution VI "Cancellation markers").
/// </summary>
public interface ICancellationMarkerRepository
{
    /// <summary>Loads the marker for <paramref name="orderId"/>, or <see langword="null"/> when the order has none.</summary>
    public Task<CancellationMarker?> GetByOrderIdAsync(Guid orderId, CancellationToken cancellationToken);

    /// <summary>Durably saves a new marker (inside the caller's order transaction).</summary>
    public Task AddAsync(CancellationMarker marker, CancellationToken cancellationToken);
}
