using Ecommerce.Payments.Domain.Orders;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// The per-order lock of <see cref="OrderUnitOfWork"/> (US-05, ADR 0004) against a real
/// PostgreSQL container.
/// </summary>
public class OrderUnitOfWorkTests : IAsyncLifetime
{
    private static readonly DateTimeOffset Now = new(2026, 11, 15, 9, 0, 0, TimeSpan.Zero);

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var context = CreateDbContext();
        await context.Database.MigrateAsync();
    }

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Second_Transaction_On_Same_Order_Waits_For_First_To_Commit()
    {
        var orderId = Guid.NewGuid();
        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();

        await using var first = await CreateUnitOfWork(contextA).BeginAsync(orderId, CancellationToken.None);
        var secondBegin = CreateUnitOfWork(contextB).BeginAsync(orderId, CancellationToken.None);

        await Task.Delay(300);
        Assert.False(secondBegin.IsCompleted);

        await first.CommitAsync(CancellationToken.None);
        await using var second = await secondBegin.WaitAsync(TimeSpan.FromSeconds(10));
        await second.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Different_Orders_Do_Not_Wait()
    {
        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();

        await using var first = await CreateUnitOfWork(contextA).BeginAsync(Guid.NewGuid(), CancellationToken.None);
        await using var second = await CreateUnitOfWork(contextB)
            .BeginAsync(Guid.NewGuid(), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(2));

        await second.CommitAsync(CancellationToken.None);
        await first.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Lock_Wait_Times_Out_With_Configured_Timeout()
    {
        var orderId = Guid.NewGuid();
        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();

        await using var first = await CreateUnitOfWork(contextA).BeginAsync(orderId, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PostgresException>(
            () => CreateUnitOfWork(contextB, lockTimeoutMs: 200).BeginAsync(orderId, CancellationToken.None));
        Assert.Equal(PostgresErrorCodes.LockNotAvailable, ex.SqlState);
    }

    [Fact]
    public async Task Dispose_Without_Commit_Rolls_Back_Marker_Insert()
    {
        var orderId = Guid.NewGuid();
        await using (var context = CreateDbContext())
        {
            await using var transaction = await CreateUnitOfWork(context).BeginAsync(orderId, CancellationToken.None);
            await new CancellationMarkerRepository(context).AddAsync(Marker(orderId), CancellationToken.None);
        }

        await using var check = CreateDbContext();
        Assert.False(await check.CancellationMarkers.AnyAsync(m => m.OrderId == orderId));
    }

    [Fact]
    public async Task Commit_Persists_Marker()
    {
        var orderId = Guid.NewGuid();
        await using (var context = CreateDbContext())
        {
            await using var transaction = await CreateUnitOfWork(context).BeginAsync(orderId, CancellationToken.None);
            await new CancellationMarkerRepository(context).AddAsync(Marker(orderId), CancellationToken.None);
            await transaction.CommitAsync(CancellationToken.None);
        }

        await using var check = CreateDbContext();
        var marker = await new CancellationMarkerRepository(check).GetByOrderIdAsync(orderId, CancellationToken.None);
        Assert.NotNull(marker);
        Assert.Equal(Now, marker.CancelledAt);
        Assert.Equal("test", marker.Reason);
    }

    [Fact]
    public async Task Retry_After_Rollback_Starts_With_Clean_ChangeTracker()
    {
        var orderId = Guid.NewGuid();
        var paymentId = Guid.NewGuid();
        await using var context = CreateDbContext();
        var unitOfWork = CreateUnitOfWork(context);

        await using (await unitOfWork.BeginAsync(orderId, CancellationToken.None))
        {
            context.Payments.Add(PaymentRow(paymentId, orderId));
        }

        Assert.Empty(context.ChangeTracker.Entries());

        await using (var retry = await unitOfWork.BeginAsync(orderId, CancellationToken.None))
        {
            context.Payments.Add(PaymentRow(paymentId, orderId));
            await context.SaveChangesAsync();
            await retry.CommitAsync(CancellationToken.None);
        }

        await using var check = CreateDbContext();
        Assert.Equal(1, await check.Payments.CountAsync(p => p.OrderId == orderId));
    }

    private static CancellationMarker Marker(Guid orderId) =>
        CancellationMarker.Record(orderId, Guid.NewGuid(), Now, Now, "test");

    private static PaymentEntity PaymentRow(Guid paymentId, Guid orderId) => new()
    {
        Id = paymentId,
        OrderId = orderId,
        CustomerId = Guid.NewGuid(),
        Amount = 10m,
        Currency = "USD",
        Status = PaymentStatus.Processed,
        SourceEventId = Guid.NewGuid(),
        CreatedAt = Now,
        ProcessedAt = Now
    };

    private static OrderUnitOfWork CreateUnitOfWork(PaymentsDbContext context, int lockTimeoutMs = 5_000) =>
        new(context, Options.Create(new PersistenceOptions { OrderLockTimeoutMs = lockTimeoutMs }));

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
