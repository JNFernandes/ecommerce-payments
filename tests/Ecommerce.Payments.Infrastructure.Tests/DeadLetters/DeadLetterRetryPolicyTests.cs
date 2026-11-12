using Ecommerce.Payments.Infrastructure.DeadLetters;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.Extensions.Time.Testing;

namespace Ecommerce.Payments.Infrastructure.Tests.DeadLetters;

public class DeadLetterRetryPolicyTests
{
    private static readonly TimeSpan[] ExpectedDefaultBackoff =
        [.. new[] { 5, 10, 20, 40, 80, 160, 320 }.Select(seconds => TimeSpan.FromSeconds(seconds))];

    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 12, 10, 0, 0, TimeSpan.Zero));

    [Fact]
    public void Should_Mark_Resolved_With_ResolvedAt_And_LastAttemptAt_On_Success()
    {
        var row = DeadLetterRows.Pending(_clock.GetUtcNow(), retryCount: 2);
        _clock.Advance(TimeSpan.FromSeconds(3));
        var now = _clock.GetUtcNow();

        DeadLetterRetryPolicy.ApplySuccess(row, now);

        Assert.Equal(DeadLetterStatus.Resolved, row.Status);
        Assert.Equal(now, row.ResolvedAt);
        Assert.Equal(now, row.LastAttemptAt);
        Assert.Equal(2, row.RetryCount);
    }

    [Fact]
    public void Should_Not_Change_Payload_Topic_Key_Or_FailureReason_On_Success()
    {
        var row = DeadLetterRows.Pending(_clock.GetUtcNow());
        var (payload, topic, key, reason, eventType) = (row.Payload, row.Topic, row.MessageKey, row.FailureReason, row.EventType);

        DeadLetterRetryPolicy.ApplySuccess(row, _clock.GetUtcNow());

        Assert.Equal(payload, row.Payload);
        Assert.Equal(topic, row.Topic);
        Assert.Equal(key, row.MessageKey);
        Assert.Equal(reason, row.FailureReason);
        Assert.Equal(eventType, row.EventType);
    }

    [Theory]
    [InlineData(1, 5)]
    [InlineData(2, 10)]
    [InlineData(3, 20)]
    [InlineData(4, 40)]
    [InlineData(7, 320)]
    public void Should_Double_Delay_From_Base_Per_Retry(int retryCount, int expectedSeconds)
    {
        var delay = DeadLetterRetryPolicy.ComputeDelay(retryCount, new DeadLetterRetryOptions());

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), delay);
    }

    [Theory]
    [InlineData(8)]
    [InlineData(20)]
    [InlineData(1_000)]
    [InlineData(int.MaxValue)]
    public void Should_Cap_Delay_At_MaxDelay(int retryCount)
    {
        var delay = DeadLetterRetryPolicy.ComputeDelay(retryCount, new DeadLetterRetryOptions());

        Assert.Equal(TimeSpan.FromMinutes(10), delay);
    }

    [Fact]
    public void Should_Never_Exceed_MaxDelay_And_Never_Decrease()
    {
        var options = new DeadLetterRetryOptions { BaseDelayMs = 1_000, MaxDelayMs = 60_000 };
        var previous = TimeSpan.Zero;

        for (var retry = 1; retry <= 50; retry++)
        {
            var delay = DeadLetterRetryPolicy.ComputeDelay(retry, options);
            Assert.True(delay >= previous);
            Assert.True(delay <= TimeSpan.FromMinutes(1));
            previous = delay;
        }
    }

    [Fact]
    public void Should_Increment_RetryCount_And_Record_LastError_And_LastAttemptAt_On_Failure()
    {
        var row = DeadLetterRows.Pending(_clock.GetUtcNow(), retryCount: 2);
        var now = _clock.GetUtcNow();

        DeadLetterRetryPolicy.ApplyFailure(row, "broker down", now, new DeadLetterRetryOptions());

        Assert.Equal(3, row.RetryCount);
        Assert.Equal("broker down", row.LastError);
        Assert.Equal(now, row.LastAttemptAt);
        Assert.Null(row.ResolvedAt);
    }

    [Fact]
    public void Should_Set_NextRetryAt_To_Now_Plus_Backoff_On_Failure()
    {
        var row = DeadLetterRows.Pending(_clock.GetUtcNow(), retryCount: 2);
        _clock.Advance(TimeSpan.FromMinutes(1));
        var now = _clock.GetUtcNow();

        DeadLetterRetryPolicy.ApplyFailure(row, "broker down", now, new DeadLetterRetryOptions());

        // Third failed retry → 5 s × 2² = 20 s.
        Assert.Equal(now.AddSeconds(20), row.NextRetryAt);
    }

    [Fact]
    public void Should_Mark_Exhausted_When_RetryCount_Reaches_MaxRetries()
    {
        var options = new DeadLetterRetryOptions { MaxRetries = 8 };
        var row = DeadLetterRows.Pending(_clock.GetUtcNow(), retryCount: 7);

        var exhausted = DeadLetterRetryPolicy.ApplyFailure(row, "broker down", _clock.GetUtcNow(), options);

        Assert.True(exhausted);
        Assert.Equal(DeadLetterStatus.Exhausted, row.Status);
        Assert.Equal(8, row.RetryCount);
    }

    [Fact]
    public void Should_Remain_PendingRetry_Below_MaxRetries()
    {
        var options = new DeadLetterRetryOptions { MaxRetries = 8 };
        var row = DeadLetterRows.Pending(_clock.GetUtcNow(), retryCount: 6);

        var exhausted = DeadLetterRetryPolicy.ApplyFailure(row, "broker down", _clock.GetUtcNow(), options);

        Assert.False(exhausted);
        Assert.Equal(DeadLetterStatus.PendingRetry, row.Status);
        Assert.Equal(7, row.RetryCount);
    }

    [Fact]
    public void Should_Exhaust_After_Exactly_MaxRetries_Failures_With_Growing_Delays()
    {
        var options = new DeadLetterRetryOptions();
        var row = DeadLetterRows.Pending(_clock.GetUtcNow());
        var delays = new List<TimeSpan>();
        var attempts = 0;

        while (row.Status == DeadLetterStatus.PendingRetry)
        {
            _clock.SetUtcNow(row.NextRetryAt!.Value);
            var now = _clock.GetUtcNow();
            attempts++;
            if (!DeadLetterRetryPolicy.ApplyFailure(row, "broker down", now, options))
            {
                delays.Add(row.NextRetryAt!.Value - now);
            }
        }

        Assert.Equal(options.MaxRetries, attempts);
        Assert.Equal(DeadLetterStatus.Exhausted, row.Status);
        Assert.Equal(ExpectedDefaultBackoff, delays);
    }
}
