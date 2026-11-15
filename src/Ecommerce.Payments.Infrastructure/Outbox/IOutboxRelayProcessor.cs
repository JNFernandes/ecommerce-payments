namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>Runs one dead-letter retry cycle. Resolved per cycle from a fresh DI scope by <see cref="OutboxRelayWorker"/>.</summary>
public interface IOutboxRelayProcessor
{
    /// <summary>
    /// Claims and republishes up to <see cref="OutboxRelayOptions.BatchSize"/> due rows,
    /// one transaction per row.
    /// </summary>
    /// <param name="cancellationToken">Stops the cycle; the row in flight is rolled back and stays due.</param>
    /// <returns>Number of rows attempted in this cycle.</returns>
    public Task<int> RunCycleAsync(CancellationToken cancellationToken);
}
