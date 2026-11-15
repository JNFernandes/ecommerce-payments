using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core / PostgreSQL implementation of <see cref="IPaymentOutboxRepository"/>.
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

        return await _dbContext.PaymentOutbox
            .FromSql($"""
                SELECT * FROM payment_outbox
                WHERE "Status" = {pending} AND "NextAttemptAt" <= {now}
                ORDER BY "NextAttemptAt"
                LIMIT {limit}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task SaveChangesAsync(CancellationToken cancellationToken) =>
        _dbContext.SaveChangesAsync(cancellationToken);
}
