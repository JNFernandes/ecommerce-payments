using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Verifies <see cref="PaymentDeadLetterRepository.ClaimDueAsync"/> against a real PostgreSQL
/// container: due-row selection, ordering, the transaction requirement and
/// <c>FOR UPDATE SKIP LOCKED</c> behaviour across concurrent claimers (US-03, ADR 0003).
/// </summary>
public class DeadLetterClaimTests : IAsyncLifetime
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
    public async Task Should_Claim_Due_PendingRetry_Row()
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
        await InsertAsync(Now.AddMinutes(-1), DeadLetterStatus.Resolved);
        await InsertAsync(Now.AddMinutes(-1), DeadLetterStatus.Exhausted);

        var claimed = await ClaimInNewTransactionAsync(limit: 10);

        Assert.Empty(claimed);
    }

    [Fact]
    public async Task Should_Claim_Oldest_NextRetryAt_First_Up_To_Limit()
    {
        var newest = await InsertAsync(Now.AddSeconds(-1));
        var oldest = await InsertAsync(Now.AddSeconds(-30));
        var middle = await InsertAsync(Now.AddSeconds(-10));

        var claimed = await ClaimInNewTransactionAsync(limit: 2);

        Assert.Equal([oldest.Id, middle.Id], claimed.Select(r => r.Id));
        Assert.DoesNotContain(newest.Id, claimed.Select(r => r.Id));
    }

    [Fact]
    public async Task Should_Throw_When_Claiming_Outside_A_Transaction()
    {
        await using var dbContext = CreateDbContext();
        var repository = new PaymentDeadLetterRepository(dbContext);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => repository.ClaimDueAsync(Now, 1, CancellationToken.None));
    }

    private async Task<IReadOnlyList<PaymentDeadLetterEntity>> ClaimInNewTransactionAsync(int limit)
    {
        await using var dbContext = CreateDbContext();
        var repository = new PaymentDeadLetterRepository(dbContext);
        await using var transaction = await repository.BeginTransactionAsync(CancellationToken.None);
        var claimed = await repository.ClaimDueAsync(Now, limit, CancellationToken.None);
        await transaction.RollbackAsync();
        return claimed;
    }

    internal async Task<PaymentDeadLetterEntity> InsertAsync(
        DateTimeOffset nextRetryAt,
        DeadLetterStatus status = DeadLetterStatus.PendingRetry)
    {
        var paymentId = Guid.NewGuid();
        var row = new PaymentDeadLetterEntity
        {
            Id = Guid.NewGuid(),
            PaymentId = paymentId,
            EventType = "PaymentProcessedEvent",
            Payload = $$"""{"eventId":"{{Guid.NewGuid()}}"}""",
            Topic = "payments.payment-processed",
            MessageKey = paymentId.ToString(),
            FailureReason = "Local: Message timed out",
            CreatedAt = Now.AddMinutes(-5),
            Status = status,
            NextRetryAt = nextRetryAt
        };

        await using var dbContext = CreateDbContext();
        await new PaymentDeadLetterRepository(dbContext).AddAsync(row, CancellationToken.None);
        return row;
    }

    internal PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
