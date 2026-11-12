using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core / PostgreSQL implementation of <see cref="IPaymentDeadLetterRepository"/>.
/// Multi-instance safety comes from <see cref="ClaimDueAsync"/>'s
/// <c>FOR UPDATE SKIP LOCKED</c>: each row is locked by exactly one open transaction at a time,
/// and other claimers skip it instead of waiting (ADR 0003).
/// </summary>
public sealed class PaymentDeadLetterRepository : IPaymentDeadLetterRepository
{
    private readonly PaymentsDbContext _dbContext;

    /// <summary>Creates the repository over a scoped <see cref="PaymentsDbContext"/>.</summary>
    public PaymentDeadLetterRepository(PaymentsDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    /// <inheritdoc />
    public async Task AddAsync(PaymentDeadLetterEntity deadLetter, CancellationToken cancellationToken)
    {
        _dbContext.PaymentDeadLetters.Add(deadLetter);
        await _dbContext.SaveChangesAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken) =>
        _dbContext.Database.BeginTransactionAsync(cancellationToken);

    /// <inheritdoc />
    public async Task<IReadOnlyList<PaymentDeadLetterEntity>> ClaimDueAsync(
        DateTimeOffset now,
        int limit,
        CancellationToken cancellationToken)
    {
        if (_dbContext.Database.CurrentTransaction is null)
        {
            throw new InvalidOperationException(
                "ClaimDueAsync must run inside a transaction; otherwise the row locks are released immediately.");
        }

        var pendingRetry = nameof(DeadLetterStatus.PendingRetry);

        return await _dbContext.PaymentDeadLetters
            .FromSql($"""
                SELECT * FROM payment_dead_letters
                WHERE "Status" = {pendingRetry} AND "NextRetryAt" <= {now}
                ORDER BY "NextRetryAt"
                LIMIT {limit}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task SaveChangesAsync(CancellationToken cancellationToken) =>
        _dbContext.SaveChangesAsync(cancellationToken);
}
