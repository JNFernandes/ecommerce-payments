using System.Globalization;
using Ecommerce.Payments.Service.Orders;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// PostgreSQL implementation of <see cref="IOrderUnitOfWork"/> (ADR 0004, Constitution VI
/// "Cancellation markers"). <see cref="BeginAsync"/> opens a transaction on the scoped
/// <see cref="PaymentsDbContext"/>, bounds lock waits with a transaction-local <c>lock_timeout</c>
/// (<see cref="PersistenceOptions.OrderLockTimeoutMs"/>) and takes
/// <c>pg_advisory_xact_lock</c> for the order as its first real statement. The lock is released
/// automatically at commit or rollback. Repositories sharing the same scoped context run inside
/// this transaction.
/// </summary>
public sealed class OrderUnitOfWork : IOrderUnitOfWork
{
    private readonly PaymentsDbContext _dbContext;
    private readonly int _lockTimeoutMs;

    /// <summary>Creates the unit of work over a scoped <see cref="PaymentsDbContext"/>.</summary>
    public OrderUnitOfWork(PaymentsDbContext dbContext, IOptions<PersistenceOptions> options)
    {
        _dbContext = dbContext;
        _lockTimeoutMs = options.Value.OrderLockTimeoutMs;
    }

    /// <inheritdoc />
    public async Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken cancellationToken)
    {
        var transaction = await _dbContext.Database.BeginTransactionAsync(cancellationToken);
        try
        {
            // set_config(..., is_local: true) is the parameterisable form of SET LOCAL.
            var lockTimeout = _lockTimeoutMs.ToString(CultureInfo.InvariantCulture) + "ms";
            await _dbContext.Database.ExecuteSqlAsync(
                $"SELECT set_config('lock_timeout', {lockTimeout}, true)",
                cancellationToken);

            var key = OrderLockKey.For(orderId);
            await _dbContext.Database.ExecuteSqlAsync(
                $"SELECT pg_advisory_xact_lock({key})",
                cancellationToken);
        }
        catch
        {
            await RollBackAsync(_dbContext, transaction);
            throw;
        }

        return new OrderTransaction(_dbContext, transaction);
    }

    private static async Task RollBackAsync(PaymentsDbContext dbContext, IDbContextTransaction transaction)
    {
        try
        {
            await transaction.RollbackAsync(CancellationToken.None);
        }
        finally
        {
            // A retry must not re-save entities tracked by the failed attempt.
            dbContext.ChangeTracker.Clear();
            await transaction.DisposeAsync();
        }
    }

    private sealed class OrderTransaction : IOrderTransaction
    {
        private readonly PaymentsDbContext _dbContext;
        private readonly IDbContextTransaction _transaction;
        private bool _committed;
        private bool _disposed;

        public OrderTransaction(PaymentsDbContext dbContext, IDbContextTransaction transaction)
        {
            _dbContext = dbContext;
            _transaction = transaction;
        }

        public async Task CommitAsync(CancellationToken cancellationToken)
        {
            await _transaction.CommitAsync(cancellationToken);
            _committed = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_committed)
            {
                await _transaction.DisposeAsync();
                return;
            }

            await RollBackAsync(_dbContext, _transaction);
        }
    }
}
