using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Infrastructure.Retention;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// <see cref="RetentionPurger"/> (US-09, ADR 0007) against a real PostgreSQL container: only
/// expired rows of enabled stores are deleted; Pending/Exhausted outbox entries are never
/// touched; backlogs are drained in batches; concurrent purgers are safe; purging a published
/// entry never blocks the relay.
/// </summary>
public class RetentionPurgerTests : IAsyncLifetime
{
    private static readonly DateTimeOffset Now = new(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var context = CreateDbContext();
        await context.Database.MigrateAsync();
    }

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    // ---- User Story 1: outbox ----

    [Fact]
    public async Task Purges_Only_Published_Outbox_Entries_Older_Than_Retention()
    {
        var expired = await AddOutboxAsync(OutboxStatus.Published, publishedDaysAgo: 31);
        var recent = await AddOutboxAsync(OutboxStatus.Published, publishedDaysAgo: 29);
        var oldPending = await AddOutboxAsync(OutboxStatus.Pending, recordedDaysAgo: 400);
        var oldExhausted = await AddOutboxAsync(OutboxStatus.Exhausted, recordedDaysAgo: 400);

        var result = await PurgeAsync(new RetentionOptions());

        Assert.Equal(1, result.OutboxEntries);
        var remaining = await OutboxIdsAsync();
        Assert.DoesNotContain(expired, remaining);
        Assert.Contains(recent, remaining);
        Assert.Contains(oldPending, remaining);
        Assert.Contains(oldExhausted, remaining);
    }

    [Fact]
    public async Task Outbox_Retention_Zero_Purges_Nothing()
    {
        await AddOutboxAsync(OutboxStatus.Published, publishedDaysAgo: 999);

        var result = await PurgeAsync(new RetentionOptions { OutboxPublishedDays = 0 });

        Assert.Equal(0, result.OutboxEntries);
        Assert.Single(await OutboxIdsAsync());
    }

    [Fact]
    public async Task Purging_Published_Earlier_Entry_Keeps_Later_Pending_Entry_Claimable()
    {
        var paymentId = Guid.NewGuid();
        await AddOutboxAsync(OutboxStatus.Published, publishedDaysAgo: 31, paymentId: paymentId);
        var later = await AddOutboxAsync(OutboxStatus.Pending, recordedDaysAgo: 0, paymentId: paymentId);

        await PurgeAsync(new RetentionOptions());

        await using var context = CreateDbContext();
        var repository = new PaymentOutboxRepository(context);
        await using var transaction = await repository.BeginTransactionAsync(CancellationToken.None);
        var claimed = await repository.ClaimDueAsync(Now, 10, CancellationToken.None);
        Assert.Equal([later], claimed.Select(e => e.Id));
    }

    [Fact]
    public async Task Migration_Creates_Retention_Indexes()
    {
        await using var context = CreateDbContext();
        var definitions = await context.Database
            .SqlQuery<string>($"""
                SELECT indexdef AS "Value" FROM pg_indexes
                WHERE indexname IN ('IX_payment_outbox_published_at', 'IX_consumed_dead_letters_failed_at',
                                    'IX_order_cancellation_markers_recorded_at')
                """)
            .ToListAsync();

        Assert.Equal(3, definitions.Count);
        Assert.Contains(definitions, d => d.Contains("IX_payment_outbox_published_at", StringComparison.Ordinal)
            && d.Contains("WHERE", StringComparison.Ordinal));
    }

    // ---- User Story 2: dead letters and markers ----

    [Fact]
    public async Task Purges_Dead_Letters_Older_Than_Retention()
    {
        var expired = await AddDeadLetterAsync(daysAgo: 91);
        var recent = await AddDeadLetterAsync(daysAgo: 89);

        var result = await PurgeAsync(new RetentionOptions());

        Assert.Equal(1, result.DeadLetters);
        await using var context = CreateDbContext();
        var remaining = await context.ConsumedDeadLetters.Select(d => d.Id).ToListAsync();
        Assert.Equal([recent], remaining);
        Assert.DoesNotContain(expired, remaining);
    }

    [Fact]
    public async Task Purges_Markers_Older_Than_Retention()
    {
        var expired = await AddMarkerAsync(daysAgo: 181);
        var recent = await AddMarkerAsync(daysAgo: 179);

        var result = await PurgeAsync(new RetentionOptions());

        Assert.Equal(1, result.Markers);
        await using var context = CreateDbContext();
        var remaining = await context.CancellationMarkers.Select(m => m.OrderId).ToListAsync();
        Assert.Equal([recent], remaining);
        Assert.DoesNotContain(expired, remaining);
    }

    [Fact]
    public async Task Each_Store_Uses_Its_Own_Retention()
    {
        await AddOutboxAsync(OutboxStatus.Published, publishedDaysAgo: 400);
        await AddDeadLetterAsync(daysAgo: 2);
        await AddMarkerAsync(daysAgo: 400);

        var result = await PurgeAsync(new RetentionOptions
        {
            OutboxPublishedDays = 0,
            ConsumedDeadLettersDays = 1,
            CancellationMarkersDays = 0
        });

        Assert.Equal(new PurgeResult(0, 1, 0), result);
    }

    // ---- User Story 3: batches and concurrency ----

    [Fact]
    public async Task Purges_Backlog_Larger_Than_Ten_Batches_In_One_Call()
    {
        for (var i = 0; i < 105; i++)
        {
            await AddDeadLetterAsync(daysAgo: 100, offset: i);
        }

        var result = await PurgeAsync(new RetentionOptions { BatchSize = 10 });

        Assert.Equal(105, result.DeadLetters);
        await using var context = CreateDbContext();
        Assert.Equal(0, await context.ConsumedDeadLetters.CountAsync());
    }

    [Fact]
    public async Task Concurrent_Purgers_Delete_Everything_Expired_Without_Errors()
    {
        await using (var context = CreateDbContext())
        {
            for (var i = 0; i < 550; i++)
            {
                context.CancellationMarkers.Add(Marker(daysAgo: i < 500 ? 200 : 10));
            }

            await context.SaveChangesAsync();
        }

        var options = new RetentionOptions { BatchSize = 20 };
        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();
        var results = await Task.WhenAll(
            Task.Run(() => CreatePurger(contextA, options).PurgeAsync(CancellationToken.None)),
            Task.Run(() => CreatePurger(contextB, options).PurgeAsync(CancellationToken.None)));

        Assert.Equal(500, results.Sum(r => r.Markers));
        await using var check = CreateDbContext();
        Assert.Equal(50, await check.CancellationMarkers.CountAsync());
    }

    // ---- Helpers ----

    private async Task<PurgeResult> PurgeAsync(RetentionOptions options)
    {
        await using var context = CreateDbContext();
        return await CreatePurger(context, options).PurgeAsync(CancellationToken.None);
    }

    private static RetentionPurger CreatePurger(PaymentsDbContext context, RetentionOptions options) =>
        new(context, Options.Create(options), new FakeTimeProvider(Now));

    private async Task<Guid> AddOutboxAsync(
        OutboxStatus status,
        int publishedDaysAgo = 0,
        int recordedDaysAgo = 0,
        Guid? paymentId = null)
    {
        var payment = paymentId ?? Guid.NewGuid();
        var recordedAt = Now.AddDays(-Math.Max(recordedDaysAgo, publishedDaysAgo));
        var entry = new OutboxEntryEntity
        {
            Id = Guid.NewGuid(),
            EventId = Guid.NewGuid(),
            PaymentId = payment,
            EventType = "PaymentProcessedEvent",
            Payload = "{}",
            Topic = "payments.payment-processed",
            MessageKey = payment.ToString(),
            Status = status,
            RecordedAt = recordedAt,
            NextAttemptAt = recordedAt,
            PublishedAt = status == OutboxStatus.Published ? Now.AddDays(-publishedDaysAgo) : null
        };

        await using var context = CreateDbContext();
        context.PaymentOutbox.Add(entry);
        await context.SaveChangesAsync();
        return entry.Id;
    }

    private async Task<List<Guid>> OutboxIdsAsync()
    {
        await using var context = CreateDbContext();
        return await context.PaymentOutbox.Select(e => e.Id).ToListAsync();
    }

    private async Task<Guid> AddDeadLetterAsync(int daysAgo, int offset = 0)
    {
        var row = new ConsumedDeadLetterEntity
        {
            Id = Guid.NewGuid(),
            Topic = "orders.order-placed",
            Partition = 0,
            Offset = (daysAgo * 1_000) + offset,
            Payload = "not json",
            Error = "test",
            Attempts = 1,
            FailedAt = Now.AddDays(-daysAgo)
        };

        await using var context = CreateDbContext();
        context.ConsumedDeadLetters.Add(row);
        await context.SaveChangesAsync();
        return row.Id;
    }

    private async Task<Guid> AddMarkerAsync(int daysAgo)
    {
        var marker = Marker(daysAgo);
        await using var context = CreateDbContext();
        context.CancellationMarkers.Add(marker);
        await context.SaveChangesAsync();
        return marker.OrderId;
    }

    private static CancellationMarkerEntity Marker(int daysAgo) => new()
    {
        OrderId = Guid.NewGuid(),
        SourceEventId = Guid.NewGuid(),
        CancelledAt = Now.AddDays(-daysAgo),
        RecordedAt = Now.AddDays(-daysAgo)
    };

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
