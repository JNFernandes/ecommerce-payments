using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>Migration <c>AddConsumedDeadLetters</c> (US-06): table, unique position index, attempts check.</summary>
public class ConsumedDeadLetterMigrationTests : IAsyncLifetime
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
    public async Task Table_And_Unique_Position_Index_Exist()
    {
        await using var context = CreateDbContext();

        var tables = await context.Database
            .SqlQuery<int>($"SELECT count(*)::int AS \"Value\" FROM information_schema.tables WHERE table_name = 'consumed_dead_letters'")
            .SingleAsync();
        var uniqueIndexes = await context.Database
            .SqlQuery<int>($"SELECT count(*)::int AS \"Value\" FROM pg_indexes WHERE tablename = 'consumed_dead_letters' AND indexname = 'IX_consumed_dead_letters_position' AND indexdef LIKE 'CREATE UNIQUE INDEX%'")
            .SingleAsync();

        Assert.Equal(1, tables);
        Assert.Equal(1, uniqueIndexes);
    }

    [Fact]
    public async Task Zero_Attempts_Violates_Check_Constraint()
    {
        await using var context = CreateDbContext();

        var ex = await Assert.ThrowsAsync<PostgresException>(() => context.Database.ExecuteSqlAsync(
            $"""
            INSERT INTO consumed_dead_letters ("Id", "Topic", "Partition", "Offset", "Error", "Attempts", "FailedAt")
            VALUES ({Guid.NewGuid()}, 't', 0, 0, 'e', 0, {DateTimeOffset.UtcNow})
            """));

        Assert.Equal(PostgresErrorCodes.CheckViolation, ex.SqlState);
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
