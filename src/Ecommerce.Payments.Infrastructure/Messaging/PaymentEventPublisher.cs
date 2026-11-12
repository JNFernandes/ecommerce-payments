using System.Text.Json;
using Confluent.Kafka;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Messaging;

/// <summary>
/// Confluent.Kafka implementation of <see cref="IPaymentEventPublisher"/> and
/// <see cref="IRawEventPublisher"/>. Serializes <see cref="PaymentProcessed"/> /
/// <see cref="PaymentFailed"/> domain events into their wire formats and publishes them through
/// <see cref="PublishRawAsync"/>. Invoked by <see cref="ProcessPaymentHandler"/> only after a
/// successful save (Constitution Principle II).
///
/// If the publish itself fails, the event is written to <c>payment_dead_letters</c> (with its
/// topic, key and retry bookkeeping) instead of being lost, and the method still returns
/// normally — the <c>Payment</c> row is already correct and durable, so the Consumer commits the
/// offset. The dead-letter retry worker later republishes it through <see cref="PublishRawAsync"/>,
/// which never dead-letters (US-03, ADR 0003).
/// </summary>
public sealed class PaymentEventPublisher : IPaymentEventPublisher, IRawEventPublisher, IDisposable
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<PaymentEventPublisher> _logger;
    private readonly IProducer<string, string> _producer;

    /// <summary>Creates the publisher and its long-lived Kafka producer (no broker connection is made here).</summary>
    public PaymentEventPublisher(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        TimeProvider timeProvider,
        ILogger<PaymentEventPublisher> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _scopeFactory = scopeFactory;
        _timeProvider = timeProvider;
        _logger = logger;
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            // Bound how long a publish attempt can hang before we know it failed and need to
            // dead-letter — the unbounded librdkafka default (5 minutes) is too long to hold a
            // message in memory before falling back.
            MessageTimeoutMs = 10_000
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    /// <inheritdoc />
    public async Task PublishAsync(PaymentProcessed paymentProcessed, CancellationToken cancellationToken)
    {
        var integrationEvent = new PaymentProcessedEvent
        {
            EventId = Guid.NewGuid(),
            OccurredAt = paymentProcessed.ProcessedAt,
            AggregateId = paymentProcessed.PaymentId,
            Version = 1,
            OrderId = paymentProcessed.OrderId,
            Amount = paymentProcessed.Amount,
            Currency = paymentProcessed.Currency,
            ProcessedAt = paymentProcessed.ProcessedAt
        };

        await PublishOrDeadLetterAsync(
            _kafkaOptions.PaymentProcessedTopic,
            paymentProcessed.PaymentId,
            paymentProcessed.OrderId,
            nameof(PaymentProcessedEvent),
            JsonSerializer.Serialize(integrationEvent),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task PublishFailedAsync(PaymentFailed paymentFailed, CancellationToken cancellationToken)
    {
        var integrationEvent = new PaymentFailedEvent
        {
            EventId = Guid.NewGuid(),
            OccurredAt = paymentFailed.FailedAt,
            AggregateId = paymentFailed.PaymentId,
            Version = 1,
            OrderId = paymentFailed.OrderId,
            Reason = paymentFailed.Reason,
            FailedAt = paymentFailed.FailedAt
        };

        await PublishOrDeadLetterAsync(
            _kafkaOptions.PaymentFailedTopic,
            paymentFailed.PaymentId,
            paymentFailed.OrderId,
            nameof(PaymentFailedEvent),
            JsonSerializer.Serialize(integrationEvent),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task PublishRawAsync(string topic, string key, string payload, CancellationToken cancellationToken)
    {
        var message = new Message<string, string> { Key = key, Value = payload };
        await _producer.ProduceAsync(topic, message, cancellationToken);
    }

    private async Task PublishOrDeadLetterAsync(
        string topic,
        Guid paymentId,
        Guid orderId,
        string eventType,
        string payload,
        CancellationToken cancellationToken)
    {
        var key = paymentId.ToString();

        try
        {
            await PublishRawAsync(topic, key, payload, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to publish {EventType} for payment {PaymentId} (order {OrderId}) to {Topic} after " +
                "a successful save; writing to payment_dead_letters instead of losing the event.",
                eventType,
                paymentId,
                orderId,
                topic);

            await WriteDeadLetterAsync(paymentId, eventType, topic, key, payload, ex, cancellationToken);
        }
    }

    private async Task WriteDeadLetterAsync(
        Guid paymentId,
        string eventType,
        string topic,
        string key,
        string payload,
        Exception failure,
        CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();

        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IPaymentDeadLetterRepository>();

        await repository.AddAsync(
            new PaymentDeadLetterEntity
            {
                Id = Guid.NewGuid(),
                PaymentId = paymentId,
                EventType = eventType,
                Payload = payload,
                Topic = topic,
                MessageKey = key,
                FailureReason = failure.Message,
                CreatedAt = now,
                Status = DeadLetterStatus.PendingRetry,
                RetryCount = 0,
                NextRetryAt = now
            },
            cancellationToken);
    }

    /// <summary>Flushes pending messages (up to 10 s) and disposes the producer.</summary>
    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
