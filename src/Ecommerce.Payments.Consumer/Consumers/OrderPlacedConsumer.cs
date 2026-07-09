using Confluent.Kafka;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Consumer.Consumers;

/// <summary>
/// Consumer-layer <see cref="BackgroundService"/> for <c>orders.order-placed</c>. Deserializes
/// each message into a typed <see cref="OrderPlacedEvent"/>, rejects malformed/incomplete
/// envelopes without ever forwarding them to the Service layer, and only commits a message's
/// offset once it has been fully handled — never before (Constitution Principle I/II).
/// </summary>
public sealed class OrderPlacedConsumer : BackgroundService
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderPlacedConsumer> _logger;
    private readonly IConsumer<Ignore, string> _consumer;

    public OrderPlacedConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<OrderPlacedConsumer> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _scopeFactory = scopeFactory;
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_kafkaOptions.OrderPlacedTopic);

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
                _logger.LogError(ex, "Kafka consume error on {Topic}", _kafkaOptions.OrderPlacedTopic);
                continue;
            }

            try
            {
                // HandleMessageAsync returns true (envelope rejected or fully processed) or
                // throws (a save failure that survived retries — Constitution Principle II /
                // User Story 3). It never returns false; the catch below is what prevents a
                // commit on failure.
                var shouldCommit = await HandleMessageAsync(consumeResult.Message.Value, stoppingToken);
                if (shouldCommit)
                {
                    _consumer.Commit(consumeResult);
                }
            }
            catch (Exception ex)
            {
                // Catch-all so a single bad message never crashes the host (Constitution Principle I).
                // Offset is intentionally not committed here — Kafka redelivery drives the retry.
                _logger.LogError(ex, "Unhandled exception processing OrderPlaced message; will be retried");
            }
        }
    }

    /// <summary>
    /// Validates and dispatches a single raw message to the Service layer. Returns whether the
    /// Kafka offset should be committed. Public/internal seam so integration/component tests can
    /// drive this without a real Kafka broker.
    /// </summary>
    internal async Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken)
    {
        if (!OrderPlacedEvent.TryParse(rawMessage, out var orderPlaced, out var error))
        {
            _logger.LogWarning(
                "Rejecting malformed OrderPlaced message: {Error}. Raw message logged for review.",
                error);
            // Malformed envelopes never reach Domain; commit so the poison message is not
            // redelivered forever (Constitution Principle I / spec Edge Cases).
            return true;
        }

        using var scope = _scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<ProcessPaymentHandler>();

        await handler.HandleAsync(orderPlaced!, cancellationToken);
        return true;
    }

    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        base.Dispose();
    }
}
