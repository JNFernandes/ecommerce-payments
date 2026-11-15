using Confluent.Kafka;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Consumer.Consumers;

/// <summary>
/// Shared Consumer-layer loop for the service's Kafka consumers (Constitution Principles I, II
/// and VI). A subclass names its <see cref="Topic"/> and implements
/// <see cref="HandleMessageAsync"/>; this base owns everything else:
/// <list type="bullet">
/// <item>Startup never depends on Kafka: <see cref="ExecuteAsync"/> yields before subscribing.</item>
/// <item>Offsets are committed manually and only after <see cref="HandleMessageAsync"/> returns
/// <see langword="true"/>, or after the message has been stored as a dead letter.</item>
/// <item>At-least-once delivery: when handling throws, the consumer seeks back to the failed
/// message and retries it after a capped exponential backoff
/// (<see cref="KafkaOptions.ConsumerRetryBaseDelayMs"/> doubling up to
/// <see cref="KafkaOptions.ConsumerRetryMaxDelayMs"/>). Later messages on that partition are
/// not processed or committed before it, so nothing is skipped.</item>
/// <item>Inbound dead-letter (US-06, ADR 0005): after
/// <see cref="KafkaOptions.ConsumerMaxAttempts"/> failed attempts — or at once for an
/// <see cref="InvalidMessageException"/> — the message is stored as received in
/// <c>consumed_dead_letters</c> (<see cref="IConsumedDeadLetterStore"/>) and only then
/// committed, so one bad message cannot stall its partition. If storing fails, nothing is
/// committed and the message keeps being retried, handling it normally first each time.</item>
/// <item>No exception from a single message ever crashes the host.</item>
/// </list>
/// </summary>
public abstract class KafkaConsumerBase : BackgroundService
{
    private readonly IConsumer<string?, string?> _consumer;

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
        _consumer = new ConsumerBuilder<string?, string?>(consumerConfig).Build();
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
    /// should be committed (message fully handled). Throws <see cref="InvalidMessageException"/>
    /// for a malformed envelope (stored as a dead letter, never retried), and any other
    /// exception when it could not be handled (retried, then dead-lettered).
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

        // The message currently failing and its failed attempts. Retry in place means
        // consecutive failures are attempts on the same message; the position check guards
        // against a rebalance handing us a different one.
        TopicPartitionOffset? failingPosition = null;
        var attempts = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string?, string?> consumeResult;
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

            if (failingPosition != consumeResult.TopicPartitionOffset)
            {
                failingPosition = consumeResult.TopicPartitionOffset;
                attempts = 0;
            }

            TimeSpan? retryDelay;
            try
            {
                if (await HandleMessageAsync(consumeResult.Message.Value ?? string.Empty, stoppingToken))
                {
                    _consumer.Commit(consumeResult);
                }

                failingPosition = null;
                attempts = 0;
                continue;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (InvalidMessageException ex)
            {
                // Can never succeed: store it once, without backoff or retries.
                retryDelay = await DeadLetterAsync(consumeResult, ex, deadLetterAttempts: 1, stoppingToken);
            }
            catch (Exception ex)
            {
                attempts++;
                if (attempts >= KafkaOptions.ConsumerMaxAttempts)
                {
                    retryDelay = await DeadLetterAsync(consumeResult, ex, attempts, stoppingToken);
                }
                else
                {
                    retryDelay = ComputeRetryDelay(attempts);
                    Logger.LogError(
                        ex,
                        "Failed to handle message on {Topic} [{Partition}] @ {Offset} (attempt {Attempt} of {MaxAttempts}); " +
                        "not committed, retrying the same message in {Delay}.",
                        Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value,
                        attempts,
                        KafkaOptions.ConsumerMaxAttempts,
                        retryDelay);
                }
            }

            if (retryDelay is null)
            {
                // Stored and committed: move on to the next message.
                failingPosition = null;
                attempts = 0;
                continue;
            }

            // Re-read the failed message next instead of moving past it, so a later commit
            // can never skip it (at-least-once, Constitution VI).
            _consumer.Seek(consumeResult.TopicPartitionOffset);

            try
            {
                await Task.Delay(retryDelay.Value, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Stores the message in <c>consumed_dead_letters</c>, then commits it. Returns
    /// <see langword="null"/> on success, or the backoff to wait before retrying when the store
    /// failed (the message is then not committed).
    /// </summary>
    private async Task<TimeSpan?> DeadLetterAsync(
        ConsumeResult<string?, string?> consumeResult,
        Exception failure,
        int deadLetterAttempts,
        CancellationToken stoppingToken)
    {
        var error = DescribeError(failure);
        try
        {
            await using (var scope = ScopeFactory.CreateAsyncScope())
            {
                var store = scope.ServiceProvider.GetRequiredService<IConsumedDeadLetterStore>();
                await store.StoreAsync(
                    new ConsumedDeadLetter(
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value,
                        consumeResult.Message.Key,
                        consumeResult.Message.Value,
                        error,
                        deadLetterAttempts),
                    stoppingToken);
            }

            _consumer.Commit(consumeResult);
        }
        catch (Exception storeException) when (storeException is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            var delay = ComputeRetryDelay(Math.Max(deadLetterAttempts, 1));
            Logger.LogError(
                storeException,
                "Could not dead-letter message on {Topic} [{Partition}] @ {Offset} ({Error}); " +
                "not committed, retrying the same message in {Delay}.",
                Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value,
                error,
                delay);
            return delay;
        }

        if (failure is InvalidMessageException invalid)
        {
            Logger.LogWarning(
                "Malformed message on {Topic} [{Partition}] @ {Offset}: {Reason}; stored in consumed_dead_letters and committed.",
                Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value,
                invalid.Reason);
        }
        else
        {
            Logger.LogError(
                failure,
                "Dead-lettered message on {Topic} [{Partition}] @ {Offset} after {Attempts} attempts: {Error}; " +
                "stored in consumed_dead_letters and committed.",
                Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value,
                deadLetterAttempts,
                error);
        }

        return null;
    }

    /// <summary>
    /// <c>"Type: message"</c> of <paramref name="exception"/>, plus its innermost exception when
    /// different (e.g. the <c>PostgresException</c> inside a <c>DbUpdateException</c>).
    /// </summary>
    internal static string DescribeError(Exception exception)
    {
        var description = $"{exception.GetType().FullName}: {exception.Message}";
        var innermost = exception.GetBaseException();
        return ReferenceEquals(innermost, exception)
            ? description
            : $"{description} ---> {innermost.GetType().FullName}: {innermost.Message}";
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
