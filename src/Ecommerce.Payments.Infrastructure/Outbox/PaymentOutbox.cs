using System.Text.Json;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>
/// PostgreSQL implementation of <see cref="IPaymentOutbox"/> (US-07, ADR 0006). Builds the
/// integration event for a domain event — a new <c>eventId</c>, <c>aggregateId</c> = payment id,
/// <c>version</c> 1, plus the event's fields — serializes it once, and adds a
/// <see cref="OutboxStatus.Pending"/> entry to <c>payment_outbox</c> on the scoped
/// <see cref="PaymentsDbContext"/>. Because that context is shared with the per-order
/// transaction, the entry commits or rolls back with the payment change. The stored payload is
/// exactly what the relay later publishes.
/// </summary>
public sealed class PaymentOutbox : IPaymentOutbox
{
    private readonly PaymentsDbContext _dbContext;
    private readonly KafkaOptions _kafkaOptions;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the outbox writer for one DI scope.</summary>
    public PaymentOutbox(PaymentsDbContext dbContext, IOptions<KafkaOptions> kafkaOptions, TimeProvider timeProvider)
    {
        _dbContext = dbContext;
        _kafkaOptions = kafkaOptions.Value;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public Task AddProcessedAsync(PaymentProcessed paymentProcessed, CancellationToken cancellationToken)
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

        return AddAsync(
            integrationEvent.EventId,
            paymentProcessed.PaymentId,
            nameof(PaymentProcessedEvent),
            _kafkaOptions.PaymentProcessedTopic,
            JsonSerializer.Serialize(integrationEvent),
            cancellationToken);
    }

    /// <inheritdoc />
    public Task AddFailedAsync(PaymentFailed paymentFailed, CancellationToken cancellationToken)
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

        return AddAsync(
            integrationEvent.EventId,
            paymentFailed.PaymentId,
            nameof(PaymentFailedEvent),
            _kafkaOptions.PaymentFailedTopic,
            JsonSerializer.Serialize(integrationEvent),
            cancellationToken);
    }

    /// <inheritdoc />
    public Task AddRefundedAsync(PaymentRefunded paymentRefunded, CancellationToken cancellationToken)
    {
        var integrationEvent = new PaymentRefundedEvent
        {
            EventId = Guid.NewGuid(),
            OccurredAt = paymentRefunded.RefundedAt,
            AggregateId = paymentRefunded.PaymentId,
            Version = 1,
            OrderId = paymentRefunded.OrderId,
            Amount = paymentRefunded.Amount,
            Currency = paymentRefunded.Currency,
            RefundedAt = paymentRefunded.RefundedAt
        };

        return AddAsync(
            integrationEvent.EventId,
            paymentRefunded.PaymentId,
            nameof(PaymentRefundedEvent),
            _kafkaOptions.PaymentRefundedTopic,
            JsonSerializer.Serialize(integrationEvent),
            cancellationToken);
    }

    private async Task AddAsync(
        Guid eventId,
        Guid paymentId,
        string eventType,
        string topic,
        string payload,
        CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        _dbContext.PaymentOutbox.Add(new OutboxEntryEntity
        {
            Id = Guid.NewGuid(),
            EventId = eventId,
            PaymentId = paymentId,
            EventType = eventType,
            Topic = topic,
            MessageKey = paymentId.ToString(),
            Payload = payload,
            Status = OutboxStatus.Pending,
            RetryCount = 0,
            RecordedAt = now,
            NextAttemptAt = now
        });
        await _dbContext.SaveChangesAsync(cancellationToken);
    }
}
