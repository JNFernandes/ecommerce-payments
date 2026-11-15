using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core / PostgreSQL implementation of <see cref="IPaymentOutboxRepository"/>. Entries are
/// claimed in recording order, never ahead of an unpublished earlier entry for the same payment.
/// Multi-instance safety comes from <see cref="ClaimDueAsync"/>'s
/// <c>FOR UPDATE SKIP LOCKED</c>: each row is locked by exactly one open transaction at a time,
/// and other claimers skip it instead of waiting (ADR 0003, ADR 0006).
/// </summary>
public sealed class PaymentOutboxRepository : IPaymentOutboxRepository
{
    private readonly PaymentsDbContext _dbContext;

    /// <summary>Creates the repository over a scoped <see cref="PaymentsDbContext"/>.</summary>
    public PaymentOutboxRepository(PaymentsDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    /// <inheritdoc />
    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken) =>
        _dbContext.Database.BeginTransactionAsync(cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<OutboxEntryEntity>> ClaimDueAsync(
        DateTimeOffset now,
        int limit,
        CancellationToken cancellationToken)
    {
        if (_dbContext.Database.CurrentTransaction is null)
        {
            throw new InvalidOperationException(
                "ClaimDueAsync must run inside a transaction; otherwise the row locks are released immediately.");
        }

        var pending = nameof(OutboxStatus.Pending);
        var published = nameof(OutboxStatus.Published);

        // Per-payment ordering (US-07 research R4): an entry is eligible only when every earlier
        // entry for its payment is Published. The NOT EXISTS reads committed state, so an earlier
        // entry locked by another relay instance (and skipped here) still blocks its successors.
        return await _dbContext.PaymentOutbox
            .FromSql($"""
                SELECT * FROM payment_outbox o
                WHERE o."Status" = {pending} AND o."NextAttemptAt" <= {now}
                  AND NOT EXISTS (
                      SELECT 1 FROM payment_outbox e
                      WHERE e."PaymentId" = o."PaymentId"
                        AND e."Sequence" < o."Sequence"
                        AND e."Status" <> {published})
                ORDER BY o."Sequence"
                LIMIT {limit}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> CountBlockedAfterAsync(Guid paymentId, long sequence, CancellationToken cancellationToken) =>
        _dbContext.PaymentOutbox.CountAsync(
            e => e.PaymentId == paymentId && e.Sequence > sequence && e.Status == OutboxStatus.Pending,
            cancellationToken);

    /// <inheritdoc />
    public Task SaveChangesAsync(CancellationToken cancellationToken) =>
        _dbContext.SaveChangesAsync(cancellationToken);
}
