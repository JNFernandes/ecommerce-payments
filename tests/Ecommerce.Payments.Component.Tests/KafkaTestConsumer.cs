using Confluent.Kafka;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Shared helper for Component test assertions that consume a topic right after a producer may
/// have just created it. Testcontainers Kafka classes run in parallel by default, and topic
/// auto-creation is not always visible to a brand-new consumer's first metadata fetch — a bare
/// <c>Consume()</c> can throw "Unknown topic or partition" in that narrow window even though the
/// topic exists moments later. This retries subscribe+consume within the overall timeout instead
/// of failing on the first attempt.
/// </summary>
internal static class KafkaTestConsumer
{
    public static ConsumeResult<Ignore, string>? SubscribeAndConsume(
        string bootstrapServers,
        string topic,
        TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(2));
                if (result is not null)
                {
                    return result;
                }
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                // Topic not visible to this consumer yet — retry until the deadline.
            }
        }

        return null;
    }
}
