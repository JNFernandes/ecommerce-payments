using System.Collections.Concurrent;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// <see cref="KafkaConsumerBase"/>'s inbound dead-letter behaviour (US-06, ADR 0005) against a
/// real broker: a message that keeps failing is stored after <c>ConsumerMaxAttempts</c> and
/// committed so the partition moves on; nothing is committed while the store fails; a message
/// that recovers in time stores nothing; a malformed message is stored once, without retry.
/// </summary>
public class KafkaConsumerBaseDeadLetterTests : IAsyncLifetime
{
    private const int MaxAttempts = 3;

    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();

    public Task InitializeAsync() => _kafka.StartAsync();

    public async Task DisposeAsync() => await _kafka.DisposeAsync();

    [Fact]
    public async Task Message_That_Always_Fails_Is_Stored_After_Max_Attempts_And_Next_Message_Is_Handled()
    {
        var (topic, groupId) = NewTopic();
        Produce(topic, ("k1", "A"), ("k1", "B"));
        var store = new RecordingConsumedDeadLetterStore();

        var consumer = await RunUntilAsync(topic, groupId, store, c => c.Calls.Contains("B"));

        Assert.Equal(["A", "A", "A", "B"], consumer.Calls.ToArray());
        var stored = Assert.Single(store.Stored);
        Assert.Equal(topic, stored.Topic);
        Assert.Equal(0, stored.Partition);
        Assert.Equal(0, stored.Offset);
        Assert.Equal("k1", stored.MessageKey);
        Assert.Equal("A", stored.Payload);
        Assert.Equal(MaxAttempts, stored.Attempts);
        Assert.Contains(nameof(InvalidOperationException), stored.Error, StringComparison.Ordinal);
        Assert.Contains("Simulated permanent failure", stored.Error, StringComparison.Ordinal);
        Assert.Equal(2, CommittedOffset(topic, groupId));
    }

    [Fact]
    public async Task Store_Failure_Keeps_Message_Uncommitted_And_Retries_Until_Stored()
    {
        var (topic, groupId) = NewTopic();
        Produce(topic, ("k1", "A"), ("k1", "B"));
        var store = new RecordingConsumedDeadLetterStore(failFirstCalls: 2);
        long? committedWhileFailing = null;

        var consumer = await RunUntilAsync(topic, groupId, store, c =>
        {
            if (store.FailedCalls == 2 && committedWhileFailing is null)
            {
                committedWhileFailing = CommittedOffset(topic, groupId);
            }

            return c.Calls.Contains("B");
        });

        Assert.True(consumer.Calls.Count(c => c == "A") > MaxAttempts);
        Assert.Equal(Offset.Unset.Value, committedWhileFailing);
        var stored = Assert.Single(store.Stored);
        Assert.True(stored.Attempts >= MaxAttempts);
        Assert.Equal(2, CommittedOffset(topic, groupId));
    }

    [Fact]
    public async Task Message_That_Recovers_Before_Max_Is_Not_Stored()
    {
        var (topic, groupId) = NewTopic();
        Produce(topic, ("k1", "R"), ("k1", "B"));
        var store = new RecordingConsumedDeadLetterStore();

        var consumer = await RunUntilAsync(topic, groupId, store, c => c.Calls.Contains("B"));

        Assert.Equal(["R", "R", "R", "B"], consumer.Calls.ToArray());
        Assert.Empty(store.Stored);
        Assert.Equal(2, CommittedOffset(topic, groupId));
    }

    private async Task<ScriptedConsumer> RunUntilAsync(
        string topic,
        string groupId,
        RecordingConsumedDeadLetterStore store,
        Func<ScriptedConsumer, bool> done)
    {
        var options = Options.Create(new KafkaOptions
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            ConsumerGroupId = groupId,
            ConsumerRetryBaseDelayMs = 50,
            ConsumerRetryMaxDelayMs = 200,
            ConsumerMaxAttempts = MaxAttempts
        });
        var services = new ServiceCollection()
            .AddSingleton<IConsumedDeadLetterStore>(store)
            .BuildServiceProvider();
        var consumer = new ScriptedConsumer(topic, options, services);

        await consumer.StartAsync(CancellationToken.None);
        try
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
            while (!done(consumer))
            {
                Assert.True(DateTime.UtcNow < deadline, "Condition not met within the timeout.");
                await Task.Delay(50);
            }

            // Let the commit that follows the last handled message land.
            await Task.Delay(300);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            consumer.Dispose();
            await services.DisposeAsync();
        }

        return consumer;
    }

    private static (string Topic, string GroupId) NewTopic() =>
        ($"dead-letter-{Guid.NewGuid():N}", $"dead-letter-{Guid.NewGuid():N}");

    private void Produce(string topic, params (string Key, string Value)[] messages)
    {
        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        foreach (var (key, value) in messages)
        {
            producer.Produce(topic, new Message<string, string> { Key = key, Value = value });
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private long CommittedOffset(string topic, string groupId)
    {
        using var reader = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = groupId
        }).Build();
        return reader.Committed([new TopicPartition(topic, 0)], TimeSpan.FromSeconds(10)).Single().Offset.Value;
    }

    /// <summary>
    /// "A" always throws; "R" throws twice then succeeds; "M" is malformed
    /// (<see cref="InvalidMessageException"/>); anything else succeeds. Records every call.
    /// </summary>
    private sealed class ScriptedConsumer(string topic, IOptions<KafkaOptions> options, IServiceProvider services)
        : KafkaConsumerBase(options, services.GetRequiredService<IServiceScopeFactory>(), NullLogger.Instance)
    {
        private int _recoveringFailures = 2;

        public ConcurrentQueue<string> Calls { get; } = new();

        protected override string Topic => topic;

        internal override Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken)
        {
            Calls.Enqueue(rawMessage);
            return rawMessage switch
            {
                "A" => throw new InvalidOperationException("Simulated permanent failure"),
                "R" when Interlocked.Decrement(ref _recoveringFailures) >= 0 =>
                    throw new InvalidOperationException("Simulated transient failure"),
                "M" => throw new InvalidMessageException("missing eventId"),
                _ => Task.FromResult(true)
            };
        }
    }

    /// <summary>Records stored dead letters; can be told to throw for its first calls.</summary>
    private sealed class RecordingConsumedDeadLetterStore(int failFirstCalls = 0) : IConsumedDeadLetterStore
    {
        private int _remainingFailures = failFirstCalls;
        private int _failedCalls;

        public ConcurrentQueue<ConsumedDeadLetter> Stored { get; } = new();

        public int FailedCalls => Volatile.Read(ref _failedCalls);

        public Task StoreAsync(ConsumedDeadLetter deadLetter, CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref _remainingFailures) >= 0)
            {
                Interlocked.Increment(ref _failedCalls);
                throw new InvalidOperationException("Simulated store outage");
            }

            Stored.Enqueue(deadLetter);
            return Task.CompletedTask;
        }
    }
}
