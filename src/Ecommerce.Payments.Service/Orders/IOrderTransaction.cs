namespace Ecommerce.Payments.Service.Orders;

/// <summary>
/// A transaction opened by <see cref="IOrderUnitOfWork.BeginAsync"/>. Disposing it without
/// calling <see cref="CommitAsync"/> rolls back; either way the per-order lock is released.
/// </summary>
public interface IOrderTransaction : IAsyncDisposable
{
    /// <summary>Commits the transaction and releases the per-order lock.</summary>
    public Task CommitAsync(CancellationToken cancellationToken);
}
