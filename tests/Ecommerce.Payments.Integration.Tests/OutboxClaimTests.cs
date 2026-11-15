using System.Collections.Concurrent;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Verifies <see cref="PaymentOutboxRepository.ClaimDueAsync"/> against a real PostgreSQL
/// container: due-row selection, ordering, the transaction requirement and
/// <c>FOR UPDATE SKIP LOCKED</c> behaviour across concurrent claimers (US-03, ADR 0003).
/// </summary>
public class OutboxClaimTests : IAsyncLifetime
{
    private static readonly DateTimeOffset Now = new(2026, 11, 12, 10, 0, 0, TimeSpan.Zero);

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var dbContext = CreateDbContext();
        await dbContext.Database.MigrateAsync();
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task Should_Claim_Due_Pending_Row()
    {
        var due = await InsertAsync(Now.AddSeconds(-1));

        var claimed = await ClaimInNewTransactionAsync(limit: 10);

        Assert.Equal([due.Id], claimed.Select(r => r.Id));
    }

    [Fact]
    public async Task Should_Not_Claim_Row_With_Future_NextRetryAt()
    {
        await InsertAsync(Now.AddSeconds(1));

        var claimed = await ClaimInNewTransactionAsync(limit: 10);

        Assert.Empty(claimed);
    }

    [Fact]
    public async Task Should_Not_Claim_Resolved_Or_Exhausted_Rows()
    {
        await InsertAsync(Now.AddMinutes(-1), OutboxStatus.Published);
        await InsertAsync(Now.AddMinutes(-1), OutboxStatus.Exhausted);

        var claimed = await ClaimInNewTransactionAsync(limit: 10);

        Assert.Empty(claimed);
    }

    [Fact]
    public async Task Should_Claim_In_Recording_Order_Up_To_Limit()
    {
        var first = await InsertAsync(Now.AddSeconds(-1));
        var second = await InsertAsync(Now.AddSeconds(-30));
        var third = await InsertAsync(Now.AddSeconds(-10));

        var claimed = await ClaimInNewTransactionAsync(limit: 2);

        Assert.Equal([first.Id, second.Id], claimed.Select(r => r.Id));
        Assert.DoesNotContain(third.Id, claimed.Select(r => r.Id));
    }

    // ---- US-07 User Story 3: per-payment ordering ----

    [Fact]
    public async Task Later_Entry_Is_Not_Claimed_While_Earlier_Is_Pending()
    {
        var paymentId = Guid.NewGuid();
        await InsertAsync(Now.AddMinutes(1), forPayment: paymentId);   // earlier, backing off
        await InsertAsync(Now.AddSeconds(-1), forPayment: paymentId);  // later, due

        Assert.Empty(await ClaimInNewTransactionAsync(limit: 10));
    }

    [Fact]
    public async Task Later_Entry_Is_Claimed_After_Earlier_Is_Published()
    {
        var paymentId = Guid.NewGuid();
        await InsertAsync(Now.AddSeconds(-5), OutboxStatus.Published, forPayment: paymentId);
        var later = await InsertAsync(Now.AddSeconds(-1), forPayment: paymentId);

        Assert.Equal([later.Id], (await ClaimInNewTransactionAsync(limit: 10)).Select(r => r.Id));
    }

    [Fact]
    public async Task Exhausted_Earlier_Entry_Blocks_Later_One()
    {
        var paymentId = Guid.NewGuid();
        var exhausted = await InsertAsync(Now.AddSeconds(-5), OutboxStatus.Exhausted, forPayment: paymentId);
        await InsertAsync(Now.AddSeconds(-1), forPayment: paymentId);

        Assert.Empty(await ClaimInNewTransactionAsync(limit: 10));

        await using var dbContext = CreateDbContext();
        Assert.Equal(1, await new PaymentOutboxRepository(dbContext)
            .CountBlockedAfterAsync(paymentId, exhausted.Sequence, CancellationToken.None));
    }

    [Fact]
    public async Task Other_Payments_Are_Not_Blocked()
    {
        var blockedPayment = Guid.NewGuid();
        await InsertAsync(Now.AddSeconds(-5), OutboxStatus.Exhausted, forPayment: blockedPayment);
        await InsertAsync(Now.AddSeconds(-1), forPayment: blockedPayment);
        var other = await InsertAsync(Now.AddSeconds(-1));

        Assert.Equal([other.Id], (await ClaimInNewTransactionAsync(limit: 10)).Select(r => r.Id));
    }

    [Fact]
    public async Task Concurrent_Claimers_Never_Take_Same_Entry_Or_Break_Order()
    {
        var p = Guid.NewGuid();
        var p1 = await InsertAsync(Now.AddSeconds(-3), forPayment: p);
        await InsertAsync(Now.AddSeconds(-2), forPayment: p);
        var q1 = await InsertAsync(Now.AddSeconds(-1));

        await using var contextA = CreateDbContext();
        var repositoryA = new PaymentOutboxRepository(contextA);
        await using var transactionA = await repositoryA.BeginTransactionAsync(CancellationToken.None);
        Assert.Equal([p1.Id], (await repositoryA.ClaimDueAsync(Now, 1, CancellationToken.None)).Select(r => r.Id));

        await using var contextB = CreateDbContext();
        var repositoryB = new PaymentOutboxRepository(contextB);
        await using var transactionB = await repositoryB.BeginTransactionAsync(CancellationToken.None);
        var claimedB = await repositoryB.ClaimDueAsync(Now, 10, CancellationToken.None);

        // P#1 is locked by A (skipped) and P#2 must wait for it: B only gets Q#1.
        Assert.Equal([q1.Id], claimedB.Select(r => r.Id));
    }

    [Fact]
    public async Task Should_Throw_When_Claiming_Outside_A_Transaction()
    {
        await using var dbContext = CreateDbContext();
        var repository = new PaymentOutboxRepository(dbContext);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => repository.ClaimDueAsync(Now, 1, CancellationToken.None));
    }

    [Fact]
    public async Task Two_Concurrent_Claimers_Get_Disjoint_Rows()
    {
        for (var i = 0; i < 6; i++)
        {
            await InsertAsync(Now.AddSeconds(-i - 1));
        }

        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();
        var repositoryA = new PaymentOutboxRepository(contextA);
        var repositoryB = new PaymentOutboxRepository(contextB);
        await using var transactionA = await repositoryA.BeginTransactionAsync(CancellationToken.None);
        await using var transactionB = await repositoryB.BeginTransactionAsync(CancellationToken.None);

        var claimedA = await repositoryA.ClaimDueAsync(Now, 3, CancellationToken.None);
        var claimedB = await repositoryB.ClaimDueAsync(Now, 3, CancellationToken.None);

        var idsA = claimedA.Select(r => r.Id).ToHashSet();
        var idsB = claimedB.Select(r => r.Id).ToHashSet();
        Assert.Equal(3, idsA.Count);
        Assert.Equal(3, idsB.Count);
        Assert.Empty(idsA.Intersect(idsB));
    }

    [Fact]
    public async Task Second_Claimer_Skips_Locked_Row_Without_Waiting()
    {
        await InsertAsync(Now.AddSeconds(-1));

        await using var contextA = CreateDbContext();
        var repositoryA = new PaymentOutboxRepository(contextA);
        await using var transactionA = await repositoryA.BeginTransactionAsync(CancellationToken.None);
        Assert.Single(await repositoryA.ClaimDueAsync(Now, 1, CancellationToken.None));

        await using var contextB = CreateDbContext();
        var repositoryB = new PaymentOutboxRepository(contextB);
        await using var transactionB = await repositoryB.BeginTransactionAsync(CancellationToken.None);
        // If SKIP LOCKED were missing, this claim would block and then fail with a lock timeout.
        await contextB.Database.ExecuteSqlRawAsync("SET LOCAL lock_timeout = '1s'");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var claimedB = await repositoryB.ClaimDueAsync(Now, 1, CancellationToken.None);
        stopwatch.Stop();

        Assert.Empty(claimedB);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1), $"Claim waited {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task Row_Is_Claimable_Again_After_First_Transaction_Rolls_Back()
    {
        var row = await InsertAsync(Now.AddSeconds(-1));

        await using (var contextA = CreateDbContext())
        {
            var repositoryA = new PaymentOutboxRepository(contextA);
            await using var transactionA = await repositoryA.BeginTransactionAsync(CancellationToken.None);
            Assert.Single(await repositoryA.ClaimDueAsync(Now, 1, CancellationToken.None));
            await transactionA.RollbackAsync();
        }

        var claimed = await ClaimInNewTransactionAsync(limit: 1);

        Assert.Equal([row.Id], claimed.Select(r => r.Id));
    }

    [Fact]
    public async Task Two_Processors_In_Parallel_Publish_Each_Row_Exactly_Once()
    {
        var rows = new List<OutboxEntryEntity>();
        for (var i = 0; i < 10; i++)
        {
            rows.Add(await InsertAsync(Now.AddSeconds(-i - 1)));
        }

        var publisher = new RecordingSlowPublisher(TimeSpan.FromMilliseconds(100));
        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();

        await Task.WhenAll(
            Task.Run(() => CreateProcessor(contextA, publisher).RunCycleAsync(CancellationToken.None)),
            Task.Run(() => CreateProcessor(contextB, publisher).RunCycleAsync(CancellationToken.None)));

        Assert.Equal(
            rows.Select(r => r.MessageKey).Order(),
            publisher.PublishedKeys.Order());

        await using var readContext = CreateDbContext();
        Assert.All(
            await readContext.PaymentOutbox.AsNoTracking().ToListAsync(),
            row => Assert.Equal(OutboxStatus.Published, row.Status));
    }

    private static OutboxRelayProcessor CreateProcessor(PaymentsDbContext dbContext, IRawEventPublisher publisher) => new(
        new PaymentOutboxRepository(dbContext),
        publisher,
        Options.Create(new OutboxRelayOptions()),
        new FakeTimeProvider(Now),
        NullLogger<OutboxRelayProcessor>.Instance);

    /// <summary>Records every publish; the delay widens the window in which both processors overlap.</summary>
    private sealed class RecordingSlowPublisher(TimeSpan delay) : IRawEventPublisher
    {
        private readonly ConcurrentQueue<string> _publishedKeys = new();

        public IEnumerable<string> PublishedKeys => _publishedKeys;

        public async Task PublishRawAsync(string topic, string key, string payload, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            _publishedKeys.Enqueue(key);
        }
    }

    private async Task<IReadOnlyList<OutboxEntryEntity>> ClaimInNewTransactionAsync(int limit)
    {
        await using var dbContext = CreateDbContext();
        var repository = new PaymentOutboxRepository(dbContext);
        await using var transaction = await repository.BeginTransactionAsync(CancellationToken.None);
        var claimed = await repository.ClaimDueAsync(Now, limit, CancellationToken.None);
        await transaction.RollbackAsync();
        return claimed;
    }

    internal async Task<OutboxEntryEntity> InsertAsync(
        DateTimeOffset nextRetryAt,
        OutboxStatus status = OutboxStatus.Pending,
        Guid? forPayment = null)
    {
        var paymentId = forPayment ?? Guid.NewGuid();
        var eventId = Guid.NewGuid();
        var row = new OutboxEntryEntity
        {
            Id = Guid.NewGuid(),
            PaymentId = paymentId,
            EventType = "PaymentProcessedEvent",
            Payload = $$"""{"eventId":"{{eventId}}"}""",
            Topic = "payments.payment-processed",
            MessageKey = paymentId.ToString(),
            EventId = eventId,
            RecordedAt = Now.AddMinutes(-5),
            Status = status,
            NextAttemptAt = nextRetryAt
        };

        await using var dbContext = CreateDbContext();
        dbContext.PaymentOutbox.Add(row);
        await dbContext.SaveChangesAsync();
        return row;
    }

    internal PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
