using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Relay-side persistence for the <c>payment_outbox</c> table (US-07, ADR 0006): claiming due
/// entries and saving their delivery bookkeeping.
/// </summary>
public interface IPaymentOutboxRepository
{
    /// <summary>
    /// Starts a database transaction on the repository's context. Claimed rows stay locked
    /// until the returned transaction is committed or rolled back.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The open transaction; the caller owns and disposes it.</returns>
    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Claims up to <paramref name="limit"/> rows that are <see cref="OutboxStatus.Pending"/>
    /// and due (<c>NextAttemptAt &lt;= now</c>), in recording order (<c>Sequence</c>), skipping any entry
    /// that has an earlier unpublished entry for the same payment (per-payment ordering), with
    /// <c>SELECT ... FOR UPDATE SKIP LOCKED</c>: rows locked by another transaction (another
    /// worker or instance) are skipped without waiting, so no row is processed twice
    /// concurrently. MUST be called inside a transaction from <see cref="BeginTransactionAsync"/>.
    /// </summary>
    /// <param name="now">Current time from the application's <see cref="TimeProvider"/>.</param>
    /// <param name="limit">Maximum number of rows to claim.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The claimed rows, tracked by the context so changes are saved by <see cref="SaveChangesAsync"/>.</returns>
    /// <exception cref="InvalidOperationException">No transaction is open.</exception>
    public Task<IReadOnlyList<OutboxEntryEntity>> ClaimDueAsync(
        DateTimeOffset now,
        int limit,
        CancellationToken cancellationToken);

    /// <summary>
    /// Counts <see cref="OutboxStatus.Pending"/> entries for <paramref name="paymentId"/> recorded
    /// after <paramref name="sequence"/> — the events waiting behind an exhausted entry.
    /// </summary>
    public Task<int> CountBlockedAfterAsync(Guid paymentId, long sequence, CancellationToken cancellationToken);

    /// <summary>Saves changes made to tracked (claimed) rows.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task SaveChangesAsync(CancellationToken cancellationToken);
}
