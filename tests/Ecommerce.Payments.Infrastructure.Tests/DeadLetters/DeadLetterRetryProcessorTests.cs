using Ecommerce.Payments.Infrastructure.DeadLetters;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace Ecommerce.Payments.Infrastructure.Tests.DeadLetters;

public class DeadLetterRetryProcessorTests
{
    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 12, 10, 0, 0, TimeSpan.Zero));
    private readonly Mock<IPaymentDeadLetterRepository> _repository = new();
    private readonly Mock<IRawEventPublisher> _publisher = new();
    private readonly Mock<IDbContextTransaction> _transaction = new();
    private readonly DeadLetterRetryOptions _options = new();

    public DeadLetterRetryProcessorTests()
    {
        _repository
            .Setup(r => r.BeginTransactionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_transaction.Object);
    }

    [Fact]
    public async Task Should_Publish_Stored_Topic_Key_And_Payload_Unchanged()
    {
        var row = DeadLetterRows.Pending(_clock.GetUtcNow());
        ClaimSequence(row);

        await CreateProcessor().RunCycleAsync(CancellationToken.None);

        _publisher.Verify(
            p => p.PublishRawAsync(row.Topic, row.MessageKey, row.Payload, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Should_Resolve_And_Commit_After_Successful_Publish()
    {
        var row = DeadLetterRows.Pending(_clock.GetUtcNow());
        ClaimSequence(row);

        var attempted = await CreateProcessor().RunCycleAsync(CancellationToken.None);

        Assert.Equal(1, attempted);
        Assert.Equal(DeadLetterStatus.Resolved, row.Status);
        Assert.Equal(_clock.GetUtcNow(), row.ResolvedAt);
        _repository.Verify(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
        // One commit for the resolved row, one for the empty claim that ends the cycle.
        _transaction.Verify(t => t.CommitAsync(It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task Should_Stop_Cycle_When_No_Due_Row_Is_Claimed()
    {
        ClaimSequence();

        var attempted = await CreateProcessor().RunCycleAsync(CancellationToken.None);

        Assert.Equal(0, attempted);
        _repository.Verify(
            r => r.ClaimDueAsync(It.IsAny<DateTimeOffset>(), It.IsAny<int>(), It.IsAny<CancellationToken>()),
            Times.Once);
        _publisher.Verify(
            p => p.PublishRawAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Should_Process_At_Most_BatchSize_Rows_Per_Cycle()
    {
        _options.BatchSize = 2;
        var now = _clock.GetUtcNow();
        ClaimSequence(DeadLetterRows.Pending(now), DeadLetterRows.Pending(now), DeadLetterRows.Pending(now));

        var attempted = await CreateProcessor().RunCycleAsync(CancellationToken.None);

        Assert.Equal(2, attempted);
        _publisher.Verify(
            p => p.PublishRawAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task Should_Pass_Fake_Clock_Now_And_Limit_One_To_Claim()
    {
        ClaimSequence();

        await CreateProcessor().RunCycleAsync(CancellationToken.None);

        _repository.Verify(r => r.ClaimDueAsync(_clock.GetUtcNow(), 1, It.IsAny<CancellationToken>()), Times.Once);
    }

    private void ClaimSequence(params PaymentDeadLetterEntity[] rows)
    {
        var sequence = _repository.SetupSequence(
            r => r.ClaimDueAsync(It.IsAny<DateTimeOffset>(), It.IsAny<int>(), It.IsAny<CancellationToken>()));
        foreach (var row in rows)
        {
            sequence = sequence.ReturnsAsync([row]);
        }

        sequence.ReturnsAsync([]);
    }

    private DeadLetterRetryProcessor CreateProcessor() => new(
        _repository.Object,
        _publisher.Object,
        Options.Create(_options),
        _clock,
        NullLogger<DeadLetterRetryProcessor>.Instance);
}
