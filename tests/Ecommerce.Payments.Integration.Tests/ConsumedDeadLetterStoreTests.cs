using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Time.Testing;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary><see cref="ConsumedDeadLetterStore"/> (US-06) against a real PostgreSQL container.</summary>
public class ConsumedDeadLetterStoreTests : IAsyncLifetime
{
    private const string Topic = "orders.order-placed";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 15, 21, 0, 0, TimeSpan.Zero));

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var context = CreateDbContext();
        await context.Database.MigrateAsync();
    }

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Stores_Every_Field_As_Received()
    {
        await StoreAsync(new ConsumedDeadLetter(Topic, 2, 41, null, "not json {", "System.Text.Json.JsonException: bad", 1));

        await using var context = CreateDbContext();
        var row = await context.ConsumedDeadLetters.AsNoTracking().SingleAsync();
        Assert.NotEqual(Guid.Empty, row.Id);
        Assert.Equal(Topic, row.Topic);
        Assert.Equal(2, row.Partition);
        Assert.Equal(41, row.Offset);
        Assert.Null(row.MessageKey);
        Assert.Equal("not json {", row.Payload);
        Assert.Equal("System.Text.Json.JsonException: bad", row.Error);
        Assert.Equal(1, row.Attempts);
        Assert.Equal(_clock.GetUtcNow(), row.FailedAt);
    }

    [Fact]
    public async Task Storing_Same_Position_Twice_Keeps_One_Row()
    {
        await StoreAsync(new ConsumedDeadLetter(Topic, 0, 7, "k", "first", "first error", 5));
        await StoreAsync(new ConsumedDeadLetter(Topic, 0, 7, "k", "second", "second error", 6));

        await using var context = CreateDbContext();
        var row = await context.ConsumedDeadLetters.AsNoTracking().SingleAsync();
        Assert.Equal("first", row.Payload);
        Assert.Equal("first error", row.Error);
        Assert.Equal(5, row.Attempts);
    }

    [Fact]
    public async Task Different_Offsets_Are_Separate_Rows()
    {
        await StoreAsync(new ConsumedDeadLetter(Topic, 0, 1, "k", "a", "e", 5));
        await StoreAsync(new ConsumedDeadLetter(Topic, 0, 2, "k", "b", "e", 5));

        await using var context = CreateDbContext();
        Assert.Equal(2, await context.ConsumedDeadLetters.CountAsync());
    }

    [Fact]
    public async Task Null_Payload_Is_Stored()
    {
        await StoreAsync(new ConsumedDeadLetter(Topic, 0, 3, "k", null, "empty value", 1));

        await using var context = CreateDbContext();
        Assert.Null((await context.ConsumedDeadLetters.AsNoTracking().SingleAsync()).Payload);
    }

    private async Task StoreAsync(ConsumedDeadLetter deadLetter)
    {
        await using var context = CreateDbContext();
        await new ConsumedDeadLetterStore(context, _clock).StoreAsync(deadLetter, CancellationToken.None);
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
