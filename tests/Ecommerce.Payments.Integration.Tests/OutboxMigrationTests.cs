using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Migrations that shaped the outbound store:
/// <list type="bullet">
/// <item><c>AddDeadLetterRetryBookkeeping</c> (US-03): pre-US-03 rows become due, with topic and key recovered.</item>
/// <item><c>TurnDeadLettersIntoOutbox</c> (US-07, ADR 0006): dead letters of every status become
/// outbox entries in place — statuses mapped, <c>EventId</c> taken from the payload,
/// <c>Sequence</c> in recording order — and the migration can be reverted.</item>
/// </list>
/// </summary>
public class OutboxMigrationTests : IAsyncLifetime
{
    private const string MigrationBeforeRetry = "20260709220809_AddPaymentFailureFields";
    private const string MigrationBeforeOutbox = "20261115205142_AddConsumedDeadLetters";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task MigrationBackfill_PreRetryRow_BecomesPendingWithTopicAndKey()
    {
        await using var dbContext = CreateDbContext();
        var migrator = dbContext.GetInfrastructure().GetRequiredService<IMigrator>();
        await migrator.MigrateAsync(MigrationBeforeRetry);

        var processedPaymentId = Guid.NewGuid();
        var failedPaymentId = Guid.NewGuid();
        var unknownPaymentId = Guid.NewGuid();
        await InsertPreRetryRowAsync(dbContext, processedPaymentId, "PaymentProcessedEvent");
        await InsertPreRetryRowAsync(dbContext, failedPaymentId, "PaymentFailedEvent");
        await InsertPreRetryRowAsync(dbContext, unknownPaymentId, "SomethingElseEvent");

        var beforeMigration = DateTimeOffset.UtcNow.AddSeconds(-5);
        await migrator.MigrateAsync();

        await using var readContext = CreateDbContext();
        var rows = await readContext.PaymentOutbox.AsNoTracking().ToListAsync();
        Assert.Equal(3, rows.Count);

        foreach (var row in rows)
        {
            Assert.Equal(OutboxStatus.Pending, row.Status);
            Assert.Equal(0, row.RetryCount);
            Assert.True(row.NextAttemptAt >= beforeMigration, "Existing rows must be due immediately.");
            Assert.Equal(row.PaymentId.ToString(), row.MessageKey);
            Assert.Null(row.LastAttemptAt);
            Assert.Equal("Local: Message timed out", row.LastError);
            Assert.Null(row.PublishedAt);
        }

        Assert.Equal("payments.payment-processed", rows.Single(r => r.PaymentId == processedPaymentId).Topic);
        Assert.Equal("payments.payment-failed", rows.Single(r => r.PaymentId == failedPaymentId).Topic);
        Assert.Equal(string.Empty, rows.Single(r => r.PaymentId == unknownPaymentId).Topic);
    }

    [Fact]
    public async Task TurnDeadLettersIntoOutbox_Converts_Every_Status_And_Backfills_EventId_And_Sequence()
    {
        await using var dbContext = CreateDbContext();
        var migrator = dbContext.GetInfrastructure().GetRequiredService<IMigrator>();
        await migrator.MigrateAsync(MigrationBeforeOutbox);

        var t0 = new DateTimeOffset(2026, 11, 1, 10, 0, 0, TimeSpan.Zero);
        var pending = await InsertDeadLetterAsync(dbContext, "PendingRetry", t0, nextRetryAt: t0, lastError: null);
        var resolved = await InsertDeadLetterAsync(dbContext, "Resolved", t0.AddMinutes(1), nextRetryAt: t0, lastError: "first retry failed");
        var exhausted = await InsertDeadLetterAsync(dbContext, "Exhausted", t0.AddMinutes(2), nextRetryAt: null, lastError: "gave up");

        await migrator.MigrateAsync();

        await using var readContext = CreateDbContext();
        var rows = await readContext.PaymentOutbox.AsNoTracking().OrderBy(r => r.Sequence).ToListAsync();
        Assert.Equal([pending.Id, resolved.Id, exhausted.Id], rows.Select(r => r.Id).ToArray());
        Assert.Equal([1L, 2L, 3L], rows.Select(r => r.Sequence).ToArray());

        Assert.Equal(OutboxStatus.Pending, rows[0].Status);
        Assert.Equal(OutboxStatus.Published, rows[1].Status);
        Assert.Equal(OutboxStatus.Exhausted, rows[2].Status);

        Assert.Equal([pending.EventId, resolved.EventId, exhausted.EventId], rows.Select(r => r.EventId).ToArray());
        Assert.Equal("Local: Message timed out", rows[0].LastError);  // copied from FailureReason
        Assert.Equal("first retry failed", rows[1].LastError);         // kept
        Assert.Equal(rows[2].RecordedAt, rows[2].NextAttemptAt);       // null NextRetryAt → RecordedAt
        Assert.Equal(t0, rows[0].RecordedAt);

        // New entries continue the sequence.
        readContext.PaymentOutbox.Add(NewEntry());
        await readContext.SaveChangesAsync();
        var newest = await readContext.PaymentOutbox.AsNoTracking().OrderByDescending(r => r.Sequence).FirstAsync();
        Assert.Equal(4, newest.Sequence);
    }

    [Fact]
    public async Task TurnDeadLettersIntoOutbox_Down_Restores_The_Dead_Letter_Table()
    {
        await using var dbContext = CreateDbContext();
        var migrator = dbContext.GetInfrastructure().GetRequiredService<IMigrator>();
        await migrator.MigrateAsync(MigrationBeforeOutbox);
        var t0 = new DateTimeOffset(2026, 11, 1, 10, 0, 0, TimeSpan.Zero);
        var row = await InsertDeadLetterAsync(dbContext, "PendingRetry", t0, nextRetryAt: t0, lastError: null);

        await migrator.MigrateAsync();
        await migrator.MigrateAsync(MigrationBeforeOutbox);

        var restored = await dbContext.Database
            .SqlQuery<string>($"""SELECT "Status" || '|' || "FailureReason" AS "Value" FROM payment_dead_letters WHERE "Id" = {row.Id}""")
            .SingleAsync();
        Assert.Equal("PendingRetry|Local: Message timed out", restored);
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private static OutboxEntryEntity NewEntry()
    {
        var paymentId = Guid.NewGuid();
        return new OutboxEntryEntity
        {
            Id = Guid.NewGuid(),
            EventId = Guid.NewGuid(),
            PaymentId = paymentId,
            EventType = "PaymentProcessedEvent",
            Payload = "{}",
            Topic = "payments.payment-processed",
            MessageKey = paymentId.ToString(),
            RecordedAt = DateTimeOffset.UtcNow,
            NextAttemptAt = DateTimeOffset.UtcNow
        };
    }

    private static Task<int> InsertPreRetryRowAsync(PaymentsDbContext dbContext, Guid paymentId, string eventType)
    {
        var payload = $$"""{"eventId":"{{Guid.NewGuid()}}"}""";
        return dbContext.Database.ExecuteSqlAsync($"""
            INSERT INTO payment_dead_letters ("Id", "PaymentId", "EventType", "Payload", "FailureReason", "CreatedAt")
            VALUES ({Guid.NewGuid()}, {paymentId}, {eventType}, {payload}::jsonb, 'Local: Message timed out', now())
            """);
    }

    private static async Task<(Guid Id, Guid EventId)> InsertDeadLetterAsync(
        PaymentsDbContext dbContext,
        string status,
        DateTimeOffset createdAt,
        DateTimeOffset? nextRetryAt,
        string? lastError)
    {
        var id = Guid.NewGuid();
        var eventId = Guid.NewGuid();
        var paymentId = Guid.NewGuid();
        var payload = $$"""{"eventId":"{{eventId}}","aggregateId":"{{paymentId}}"}""";
        var key = paymentId.ToString();
        await dbContext.Database.ExecuteSqlAsync($"""
            INSERT INTO payment_dead_letters
                ("Id", "PaymentId", "EventType", "Payload", "FailureReason", "CreatedAt", "Topic", "MessageKey",
                 "Status", "RetryCount", "NextRetryAt", "LastError")
            VALUES ({id}, {paymentId}, 'PaymentProcessedEvent', {payload}::jsonb, 'Local: Message timed out', {createdAt},
                    'payments.payment-processed', {key}, {status}, 0, {nextRetryAt}, {lastError})
            """);
        return (id, eventId);
    }
}
