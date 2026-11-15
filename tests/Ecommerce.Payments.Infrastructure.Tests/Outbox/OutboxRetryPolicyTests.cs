using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.Extensions.Time.Testing;

namespace Ecommerce.Payments.Infrastructure.Tests.Outbox;

public class OutboxRetryPolicyTests
{
    private static readonly TimeSpan[] ExpectedDefaultBackoff =
        [.. new[] { 5, 10, 20, 40, 80, 160, 320 }.Select(seconds => TimeSpan.FromSeconds(seconds))];

    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 12, 10, 0, 0, TimeSpan.Zero));

    [Fact]
    public void Should_Mark_Resolved_With_ResolvedAt_And_LastAttemptAt_On_Success()
    {
        var row = OutboxRows.Pending(_clock.GetUtcNow(), retryCount: 2);
        _clock.Advance(TimeSpan.FromSeconds(3));
        var now = _clock.GetUtcNow();

        OutboxRetryPolicy.ApplySuccess(row, now);

        Assert.Equal(OutboxStatus.Published, row.Status);
        Assert.Equal(now, row.PublishedAt);
        Assert.Equal(now, row.LastAttemptAt);
        Assert.Equal(2, row.RetryCount);
    }

    [Fact]
    public void Should_Not_Change_Payload_Topic_Key_Or_EventId_On_Success()
    {
        var row = OutboxRows.Pending(_clock.GetUtcNow());
        var (payload, topic, key, eventId, eventType) = (row.Payload, row.Topic, row.MessageKey, row.EventId, row.EventType);

        OutboxRetryPolicy.ApplySuccess(row, _clock.GetUtcNow());

        Assert.Equal(payload, row.Payload);
        Assert.Equal(topic, row.Topic);
        Assert.Equal(key, row.MessageKey);
        Assert.Equal(eventId, row.EventId);
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
        var delay = OutboxRetryPolicy.ComputeDelay(retryCount, new OutboxRelayOptions());

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), delay);
    }

    [Theory]
    [InlineData(8)]
    [InlineData(20)]
    [InlineData(1_000)]
    [InlineData(int.MaxValue)]
    public void Should_Cap_Delay_At_MaxDelay(int retryCount)
    {
        var delay = OutboxRetryPolicy.ComputeDelay(retryCount, new OutboxRelayOptions());

        Assert.Equal(TimeSpan.FromMinutes(10), delay);
    }

    [Fact]
    public void Should_Never_Exceed_MaxDelay_And_Never_Decrease()
    {
        var options = new OutboxRelayOptions { BaseDelayMs = 1_000, MaxDelayMs = 60_000 };
        var previous = TimeSpan.Zero;

        for (var retry = 1; retry <= 50; retry++)
        {
            var delay = OutboxRetryPolicy.ComputeDelay(retry, options);
            Assert.True(delay >= previous);
            Assert.True(delay <= TimeSpan.FromMinutes(1));
            previous = delay;
        }
    }

    [Fact]
    public void Should_Increment_RetryCount_And_Record_LastError_And_LastAttemptAt_On_Failure()
    {
        var row = OutboxRows.Pending(_clock.GetUtcNow(), retryCount: 2);
        var now = _clock.GetUtcNow();

        OutboxRetryPolicy.ApplyFailure(row, "broker down", now, new OutboxRelayOptions());

        Assert.Equal(3, row.RetryCount);
        Assert.Equal("broker down", row.LastError);
        Assert.Equal(now, row.LastAttemptAt);
        Assert.Null(row.PublishedAt);
    }

    [Fact]
    public void Should_Set_NextRetryAt_To_Now_Plus_Backoff_On_Failure()
    {
        var row = OutboxRows.Pending(_clock.GetUtcNow(), retryCount: 2);
        _clock.Advance(TimeSpan.FromMinutes(1));
        var now = _clock.GetUtcNow();

        OutboxRetryPolicy.ApplyFailure(row, "broker down", now, new OutboxRelayOptions());

        // Third failed retry → 5 s × 2² = 20 s.
        Assert.Equal(now.AddSeconds(20), row.NextAttemptAt);
    }

    [Fact]
    public void Should_Mark_Exhausted_When_RetryCount_Reaches_MaxRetries()
    {
        var options = new OutboxRelayOptions { MaxRetries = 8 };
        var row = OutboxRows.Pending(_clock.GetUtcNow(), retryCount: 7);

        var exhausted = OutboxRetryPolicy.ApplyFailure(row, "broker down", _clock.GetUtcNow(), options);

        Assert.True(exhausted);
        Assert.Equal(OutboxStatus.Exhausted, row.Status);
        Assert.Equal(8, row.RetryCount);
    }

    [Fact]
    public void Should_Remain_Pending_Below_MaxRetries()
    {
        var options = new OutboxRelayOptions { MaxRetries = 8 };
        var row = OutboxRows.Pending(_clock.GetUtcNow(), retryCount: 6);

        var exhausted = OutboxRetryPolicy.ApplyFailure(row, "broker down", _clock.GetUtcNow(), options);

        Assert.False(exhausted);
        Assert.Equal(OutboxStatus.Pending, row.Status);
        Assert.Equal(7, row.RetryCount);
    }

    [Fact]
    public void Should_Exhaust_After_Exactly_MaxRetries_Failures_With_Growing_Delays()
    {
        var options = new OutboxRelayOptions();
        var row = OutboxRows.Pending(_clock.GetUtcNow());
        var delays = new List<TimeSpan>();
        var attempts = 0;

        while (row.Status == OutboxStatus.Pending)
        {
            _clock.SetUtcNow(row.NextAttemptAt);
            var now = _clock.GetUtcNow();
            attempts++;
            if (!OutboxRetryPolicy.ApplyFailure(row, "broker down", now, options))
            {
                delays.Add(row.NextAttemptAt - now);
            }
        }

        Assert.Equal(options.MaxRetries, attempts);
        Assert.Equal(OutboxStatus.Exhausted, row.Status);
        Assert.Equal(ExpectedDefaultBackoff, delays);
    }
}
