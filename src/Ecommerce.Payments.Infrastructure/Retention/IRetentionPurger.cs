namespace Ecommerce.Payments.Infrastructure.Retention;

/// <summary>
/// Deletes expired rows from the stores covered by the retention policy (US-09, ADR 0007):
/// <c>Published</c> outbox entries, inbound dead letters and cancellation markers. Resolved per
/// run from a fresh DI scope by <see cref="RetentionWorker"/>.
/// </summary>
public interface IRetentionPurger
{
    /// <summary>
    /// Purges every enabled store, in bounded batches, until no expired rows remain; safe to run
    /// concurrently on several instances.
    /// </summary>
    /// <returns>Rows deleted per store.</returns>
    public Task<PurgeResult> PurgeAsync(CancellationToken cancellationToken);
}
