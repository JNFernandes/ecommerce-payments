using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Persistence for the <c>payment_dead_letters</c> table: written by the publisher when a
/// publish fails after a successful save, and read/updated by the dead-letter retry worker
/// (US-03, ADR 0003). The single owner of that table.
/// </summary>
public interface IPaymentDeadLetterRepository
{
    /// <summary>Inserts a new dead-letter row and saves it immediately.</summary>
    /// <param name="deadLetter">The row to insert, with its retry bookkeeping initialised.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task AddAsync(PaymentDeadLetterEntity deadLetter, CancellationToken cancellationToken);

    /// <summary>
    /// Starts a database transaction on the repository's context. Claimed rows stay locked
    /// until the returned transaction is committed or rolled back.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The open transaction; the caller owns and disposes it.</returns>
    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Claims up to <paramref name="limit"/> rows that are <see cref="DeadLetterStatus.PendingRetry"/>
    /// and due (<c>NextRetryAt &lt;= now</c>), oldest due first, with
    /// <c>SELECT ... FOR UPDATE SKIP LOCKED</c>: rows locked by another transaction (another
    /// worker or instance) are skipped without waiting, so no row is processed twice
    /// concurrently. MUST be called inside a transaction from <see cref="BeginTransactionAsync"/>.
    /// </summary>
    /// <param name="now">Current time from the application's <see cref="TimeProvider"/>.</param>
    /// <param name="limit">Maximum number of rows to claim.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The claimed rows, tracked by the context so changes are saved by <see cref="SaveChangesAsync"/>.</returns>
    /// <exception cref="InvalidOperationException">No transaction is open.</exception>
    public Task<IReadOnlyList<PaymentDeadLetterEntity>> ClaimDueAsync(
        DateTimeOffset now,
        int limit,
        CancellationToken cancellationToken);

    /// <summary>Saves changes made to tracked (claimed) rows.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task SaveChangesAsync(CancellationToken cancellationToken);
}
