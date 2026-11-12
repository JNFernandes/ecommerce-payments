using System.Collections.Concurrent;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Verifies <see cref="KafkaConsumerBase"/>'s at-least-once behaviour against a real broker
/// (Constitution VI, spec FR-010): a message whose handling throws is consumed again from the
/// same offset after a backoff, later messages wait behind it, and the committed offset never
/// moves past it until it succeeds.
/// </summary>
public class KafkaConsumerBaseRetryTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();

    public Task InitializeAsync() => _kafka.StartAsync();

    public async Task DisposeAsync() => await _kafka.DisposeAsync();

    [Fact]
    public async Task FailingMessage_Is_Retried_In_Place_And_Committed_Only_After_Success()
    {
        var topic = $"consumer-base-retry-{Guid.NewGuid():N}";
        var groupId = $"consumer-base-retry-{Guid.NewGuid():N}";
        Produce(topic, "A", "B");

        var options = Options.Create(new KafkaOptions
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            ConsumerGroupId = groupId,
            ConsumerRetryBaseDelayMs = 100,
            ConsumerRetryMaxDelayMs = 1_000
        });
        using var services = new ServiceCollection().BuildServiceProvider();
        using var consumer = new FailFirstAttemptsConsumer(topic, failuresForA: 2, options, services);

        await consumer.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntilAsync(() => consumer.Calls.Contains("B"), TimeSpan.FromSeconds(60));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }

        Assert.Equal(["A", "A", "A", "B"], consumer.Calls.ToArray());
        Assert.Equal(2, CommittedOffset(topic, groupId));
    }

    [Theory]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    [InlineData(4, 800)]
    [InlineData(5, 1_000)]
    [InlineData(1_000, 1_000)]
    public void Retry_Delay_Doubles_From_Base_And_Is_Capped(int failures, int expectedMs)
    {
        // Never subscribed or started; a group id is still required to build the consumer.
        var options = Options.Create(new KafkaOptions
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            ConsumerGroupId = "unused",
            ConsumerRetryBaseDelayMs = 100,
            ConsumerRetryMaxDelayMs = 1_000
        });
        using var services = new ServiceCollection().BuildServiceProvider();
        using var consumer = new FailFirstAttemptsConsumer("unused", failuresForA: 0, options, services);

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), consumer.ComputeRetryDelay(failures));
    }

    private void Produce(string topic, params string[] values)
    {
        using var producer = new ProducerBuilder<Null, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        foreach (var value in values)
        {
            producer.Produce(topic, new Message<Null, string> { Value = value });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private long CommittedOffset(string topic, string groupId)
    {
        using var reader = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = groupId
        }).Build();
        var committed = reader.Committed([new TopicPartition(topic, 0)], TimeSpan.FromSeconds(10));
        return committed.Single().Offset.Value;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met within the timeout.");
            await Task.Delay(50);
        }
    }

    /// <summary>Throws for message "A" the first <c>failuresForA</c> times; records every call.</summary>
    private sealed class FailFirstAttemptsConsumer(
        string topic,
        int failuresForA,
        IOptions<KafkaOptions> options,
        IServiceProvider services)
        : KafkaConsumerBase(options, services.GetRequiredService<IServiceScopeFactory>(), NullLogger.Instance)
    {
        private int _remainingFailures = failuresForA;

        public ConcurrentQueue<string> Calls { get; } = new();

        protected override string Topic => topic;

        internal override Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken)
        {
            Calls.Enqueue(rawMessage);
            if (rawMessage == "A" && Interlocked.Decrement(ref _remainingFailures) >= 0)
            {
                throw new InvalidOperationException("Simulated database outage");
            }

            return Task.FromResult(true);
        }
    }
}
