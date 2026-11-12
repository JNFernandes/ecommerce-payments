using Confluent.Kafka;
using Ecommerce.Payments.Infrastructure.Messaging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Consumer.Consumers;

/// <summary>
/// Shared Consumer-layer loop for the service's Kafka consumers (Constitution Principles I, II
/// and VI). A subclass names its <see cref="Topic"/> and implements
/// <see cref="HandleMessageAsync"/>; this base owns everything else:
/// <list type="bullet">
/// <item>Startup never depends on Kafka: <see cref="ExecuteAsync"/> yields before subscribing.</item>
/// <item>Offsets are committed manually and only after <see cref="HandleMessageAsync"/> returns
/// <see langword="true"/>.</item>
/// <item>At-least-once delivery: when handling throws, the consumer seeks back to the failed
/// message and retries it after a capped exponential backoff
/// (<see cref="KafkaOptions.ConsumerRetryBaseDelayMs"/> doubling up to
/// <see cref="KafkaOptions.ConsumerRetryMaxDelayMs"/>). Later messages on that partition are
/// not processed or committed before it, so nothing is skipped.</item>
/// <item>No exception from a single message ever crashes the host.</item>
/// </list>
/// </summary>
public abstract class KafkaConsumerBase : BackgroundService
{
    private readonly IConsumer<Ignore, string> _consumer;

    /// <summary>Builds the Kafka consumer (no broker connection is made here).</summary>
    protected KafkaConsumerBase(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        ILogger logger)
    {
        KafkaOptions = kafkaOptions.Value;
        ScopeFactory = scopeFactory;
        Logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = KafkaOptions.BootstrapServers,
            GroupId = KafkaOptions.ConsumerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
    }

    /// <summary>Bound <c>Kafka</c> configuration section.</summary>
    protected KafkaOptions KafkaOptions { get; }

    /// <summary>Creates a DI scope per message for the Service layer.</summary>
    protected IServiceScopeFactory ScopeFactory { get; }

    /// <summary>Logger of the concrete consumer.</summary>
    protected ILogger Logger { get; }

    /// <summary>The topic this consumer subscribes to.</summary>
    protected abstract string Topic { get; }

    /// <summary>
    /// Validates and handles one raw message. Returns <see langword="true"/> when the offset
    /// should be committed (message fully handled, or rejected as malformed). Throws when it
    /// could not be handled; the base then retries the same message after a backoff.
    /// Internal seam so integration/component tests can drive it without a broker.
    /// </summary>
    internal abstract Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Return control to the host before touching Kafka (Constitution VI: startup must not
        // depend on the broker being reachable).
        await Task.Yield();

        _consumer.Subscribe(Topic);
        var consecutiveFailures = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<Ignore, string> consumeResult;
            try
            {
                consumeResult = _consumer.Consume(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                // A bad message at the transport level must never crash the host.
                Logger.LogError(ex, "Kafka consume error on {Topic}", Topic);
                continue;
            }

            try
            {
                if (await HandleMessageAsync(consumeResult.Message.Value, stoppingToken))
                {
                    _consumer.Commit(consumeResult);
                }

                consecutiveFailures = 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                consecutiveFailures++;
                var delay = ComputeRetryDelay(consecutiveFailures);

                Logger.LogError(
                    ex,
                    "Failed to handle message on {Topic} [{Partition}] @ {Offset} (attempt {Attempt}); " +
                    "not committed, retrying the same message in {Delay}.",
                    Topic,
                    consumeResult.Partition.Value,
                    consumeResult.Offset.Value,
                    consecutiveFailures,
                    delay);

                // Re-read the failed message next instead of moving past it, so a later commit
                // can never skip it (at-least-once, Constitution VI).
                _consumer.Seek(consumeResult.TopicPartitionOffset);

                try
                {
                    await Task.Delay(delay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Backoff before re-consuming a failed message:
    /// <c>min(ConsumerRetryBaseDelayMs × 2^(failures − 1), ConsumerRetryMaxDelayMs)</c>.
    /// </summary>
    internal TimeSpan ComputeRetryDelay(int consecutiveFailures)
    {
        var exponent = Math.Min(Math.Max(consecutiveFailures - 1, 0), 30);
        var delayMs = Math.Min(
            (double)KafkaOptions.ConsumerRetryBaseDelayMs * Math.Pow(2, exponent),
            KafkaOptions.ConsumerRetryMaxDelayMs);
        return TimeSpan.FromMilliseconds(delayMs);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }
}
