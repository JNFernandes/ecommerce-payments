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
/// Confluent.Kafka implementation of <see cref="IPaymentEventPublisher"/>. Serializes the
/// <see cref="PaymentProcessed"/> domain event into the <see cref="PaymentProcessedEvent"/>
/// wire format and publishes it to <c>payments.payment-processed</c>. Invoked by
/// <see cref="ProcessPaymentHandler"/> only after a successful save (Constitution Principle II).
///
/// If the publish itself fails, the event is written to <c>payment_dead_letters</c> instead of
/// being lost, and this method still returns normally — the <c>Payment</c> row is already
/// correct and durable, so the Consumer commits the offset rather than reprocessing the whole
/// <c>OrderPlaced</c> message just to retry a publish (Constitution Principle II).
/// </summary>
public sealed class PaymentEventPublisher : IPaymentEventPublisher, IDisposable
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PaymentEventPublisher> _logger;
    private readonly IProducer<string, string> _producer;

    public PaymentEventPublisher(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<PaymentEventPublisher> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _scopeFactory = scopeFactory;
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

        var payload = JsonSerializer.Serialize(integrationEvent);

        try
        {
            var message = new Message<string, string>
            {
                Key = paymentProcessed.PaymentId.ToString(),
                Value = payload
            };

            await _producer.ProduceAsync(_kafkaOptions.PaymentProcessedTopic, message, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to publish PaymentProcessed for payment {PaymentId} (order {OrderId}) after " +
                "a successful save; writing to payment_dead_letters instead of losing the event.",
                paymentProcessed.PaymentId,
                paymentProcessed.OrderId);

            await WriteDeadLetterAsync(paymentProcessed.PaymentId, nameof(PaymentProcessedEvent), payload, ex, cancellationToken);
        }
    }

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

        var payload = JsonSerializer.Serialize(integrationEvent);

        try
        {
            var message = new Message<string, string>
            {
                Key = paymentFailed.PaymentId.ToString(),
                Value = payload
            };

            await _producer.ProduceAsync(_kafkaOptions.PaymentFailedTopic, message, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to publish PaymentFailed for payment {PaymentId} (order {OrderId}) after " +
                "a successful save; writing to payment_dead_letters instead of losing the event.",
                paymentFailed.PaymentId,
                paymentFailed.OrderId);

            await WriteDeadLetterAsync(paymentFailed.PaymentId, nameof(PaymentFailedEvent), payload, ex, cancellationToken);
        }
    }

    private async Task WriteDeadLetterAsync(
        Guid paymentId,
        string eventType,
        string payload,
        Exception failure,
        CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();

        dbContext.PaymentDeadLetters.Add(new PaymentDeadLetterEntity
        {
            Id = Guid.NewGuid(),
            PaymentId = paymentId,
            EventType = eventType,
            Payload = payload,
            FailureReason = failure.Message,
            CreatedAt = DateTimeOffset.UtcNow
        });

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
