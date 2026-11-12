using Ecommerce.Payments.Infrastructure.DeadLetters;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.Extensions.Time.Testing;

namespace Ecommerce.Payments.Infrastructure.Tests.DeadLetters;

public class DeadLetterRetryPolicyTests
{
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
}
