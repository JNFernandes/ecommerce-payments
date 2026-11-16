using Ecommerce.Payments.Infrastructure.Retention;

namespace Ecommerce.Payments.Infrastructure.Tests.Retention;

public class RetentionOptionsTests
{
    [Fact]
    public void Defaults_Are_Valid()
    {
        var options = new RetentionOptions();

        Assert.True(options.IsValid());
        Assert.Equal(3_600_000, options.IntervalMs);
        Assert.Equal(1_000, options.BatchSize);
        Assert.Equal(30, options.OutboxPublishedDays);
        Assert.Equal(90, options.ConsumedDeadLettersDays);
        Assert.Equal(180, options.CancellationMarkersDays);
    }

    [Fact]
    public void Zero_Days_Keep_Forever_And_Are_Valid()
    {
        var options = new RetentionOptions { OutboxPublishedDays = 0, ConsumedDeadLettersDays = 0, CancellationMarkersDays = 0 };

        Assert.True(options.IsValid());
    }

    [Theory]
    [InlineData(0, 1_000)]
    [InlineData(-1, 1_000)]
    [InlineData(1_000, 0)]
    [InlineData(1_000, -5)]
    public void Non_Positive_Interval_Or_Batch_Is_Invalid(int intervalMs, int batchSize)
    {
        Assert.False(new RetentionOptions { IntervalMs = intervalMs, BatchSize = batchSize }.IsValid());
    }

    [Theory]
    [InlineData(-1, 90, 180)]
    [InlineData(30, -1, 180)]
    [InlineData(30, 90, -1)]
    public void Negative_Days_Are_Invalid(int outbox, int deadLetters, int markers)
    {
        var options = new RetentionOptions
        {
            OutboxPublishedDays = outbox,
            ConsumedDeadLettersDays = deadLetters,
            CancellationMarkersDays = markers
        };

        Assert.False(options.IsValid());
    }
}
