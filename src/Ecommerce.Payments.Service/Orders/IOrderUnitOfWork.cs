namespace Ecommerce.Payments.Service.Orders;

/// <summary>
/// Serialises all work on one order (ADR 0004). <see cref="BeginAsync"/> opens a database
/// transaction that holds a per-order lock until it is committed or disposed; a second caller
/// for the same order waits until then and afterwards sees the first one's writes. Every handler
/// that reads or changes an order's state runs its read-decide-write step inside one of these
/// (Constitution VI "Cancellation markers"). Publishing to Kafka happens only after commit.
/// </summary>
public interface IOrderUnitOfWork
{
    /// <summary>Opens the transaction and takes the lock for <paramref name="orderId"/>.</summary>
    public Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken cancellationToken);
}
