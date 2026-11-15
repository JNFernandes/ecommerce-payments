namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>Runs one outbox relay cycle. Resolved per cycle from a fresh DI scope by <see cref="OutboxRelayWorker"/>.</summary>
public interface IOutboxRelayProcessor
{
    /// <summary>
    /// Claims and publishes up to <see cref="OutboxRelayOptions.BatchSize"/> due entries, one
    /// transaction per entry, stopping early when nothing more is due.
    /// </summary>
    /// <param name="cancellationToken">Stops the cycle; the entry in flight is rolled back and stays due.</param>
    /// <returns>Number of entries attempted in this cycle.</returns>
    public Task<int> RunCycleAsync(CancellationToken cancellationToken);
}
