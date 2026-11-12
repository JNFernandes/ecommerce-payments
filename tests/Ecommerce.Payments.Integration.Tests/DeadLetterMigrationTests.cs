using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Verifies the <c>AddDeadLetterRetryBookkeeping</c> migration backfill (spec FR-002,
/// data-model.md): dead letters written before US-03 become due <c>PendingRetry</c> rows with
/// their topic and key recovered from the event type and payment id.
/// </summary>
public class DeadLetterMigrationTests : IAsyncLifetime
{
    private const string MigrationBeforeRetry = "20260709220809_AddPaymentFailureFields";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task MigrationBackfill_ExistingRow_BecomesPendingRetryWithTopicAndKey()
    {
        await using var dbContext = CreateDbContext();
        var migrator = dbContext.GetInfrastructure().GetRequiredService<IMigrator>();
        await migrator.MigrateAsync(MigrationBeforeRetry);

        var processedPaymentId = Guid.NewGuid();
        var failedPaymentId = Guid.NewGuid();
        var unknownPaymentId = Guid.NewGuid();
        await InsertLegacyRowAsync(dbContext, processedPaymentId, "PaymentProcessedEvent");
        await InsertLegacyRowAsync(dbContext, failedPaymentId, "PaymentFailedEvent");
        await InsertLegacyRowAsync(dbContext, unknownPaymentId, "SomethingElseEvent");

        var beforeMigration = DateTimeOffset.UtcNow.AddSeconds(-5);
        await migrator.MigrateAsync();

        await using var readContext = CreateDbContext();
        var rows = await readContext.PaymentDeadLetters.AsNoTracking().ToListAsync();
        Assert.Equal(3, rows.Count);

        foreach (var row in rows)
        {
            Assert.Equal(DeadLetterStatus.PendingRetry, row.Status);
            Assert.Equal(0, row.RetryCount);
            Assert.NotNull(row.NextRetryAt);
            Assert.True(row.NextRetryAt >= beforeMigration, "Existing rows must be due immediately.");
            Assert.Equal(row.PaymentId.ToString(), row.MessageKey);
            Assert.Null(row.LastAttemptAt);
            Assert.Null(row.LastError);
            Assert.Null(row.ResolvedAt);
        }

        Assert.Equal("payments.payment-processed", rows.Single(r => r.PaymentId == processedPaymentId).Topic);
        Assert.Equal("payments.payment-failed", rows.Single(r => r.PaymentId == failedPaymentId).Topic);
        Assert.Equal(string.Empty, rows.Single(r => r.PaymentId == unknownPaymentId).Topic);
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private static Task<int> InsertLegacyRowAsync(PaymentsDbContext dbContext, Guid paymentId, string eventType)
    {
        var payload = $$"""{"eventId":"{{Guid.NewGuid()}}"}""";
        return dbContext.Database.ExecuteSqlAsync($"""
            INSERT INTO payment_dead_letters ("Id", "PaymentId", "EventType", "Payload", "FailureReason", "CreatedAt")
            VALUES ({Guid.NewGuid()}, {paymentId}, {eventType}, {payload}::jsonb, 'Local: Message timed out', now())
            """);
    }
}
