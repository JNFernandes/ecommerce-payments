using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>Migration <c>AddOrderCancellationMarkers</c> (US-05): table and one-marker-per-order key.</summary>
public class CancellationMarkerMigrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var context = CreateDbContext();
        await context.Database.MigrateAsync();
    }

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Table_Exists_After_Migration()
    {
        await using var context = CreateDbContext();

        var count = await context.Database
            .SqlQuery<int>($"SELECT count(*)::int AS \"Value\" FROM information_schema.tables WHERE table_name = 'order_cancellation_markers'")
            .SingleAsync();

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Second_Marker_For_Same_Order_Violates_Primary_Key()
    {
        var orderId = Guid.NewGuid();
        await using (var context = CreateDbContext())
        {
            context.CancellationMarkers.Add(Row(orderId));
            await context.SaveChangesAsync();
        }

        await using var second = CreateDbContext();
        second.CancellationMarkers.Add(Row(orderId));
        var ex = await Assert.ThrowsAsync<DbUpdateException>(() => second.SaveChangesAsync());

        var postgres = Assert.IsType<PostgresException>(ex.InnerException);
        Assert.Equal(PostgresErrorCodes.UniqueViolation, postgres.SqlState);
    }

    private static CancellationMarkerEntity Row(Guid orderId) => new()
    {
        OrderId = orderId,
        SourceEventId = Guid.NewGuid(),
        CancelledAt = DateTimeOffset.UtcNow,
        RecordedAt = DateTimeOffset.UtcNow
    };

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
