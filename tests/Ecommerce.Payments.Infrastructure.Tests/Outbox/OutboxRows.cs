using Ecommerce.Payments.Infrastructure.Persistence.Entities;

namespace Ecommerce.Payments.Infrastructure.Tests.Outbox;

/// <summary>Builds outbox entries for unit tests.</summary>
internal static class OutboxRows
{
    public static OutboxEntryEntity Pending(DateTimeOffset now, int retryCount = 0)
    {
        var paymentId = Guid.NewGuid();
        var eventId = Guid.NewGuid();
        return new OutboxEntryEntity
        {
            Id = Guid.NewGuid(),
            PaymentId = paymentId,
            EventType = "PaymentProcessedEvent",
            Payload = $$"""{"eventId":"{{eventId}}","aggregateId":"{{paymentId}}"}""",
            Topic = "payments.payment-processed",
            MessageKey = paymentId.ToString(),
            EventId = eventId,
            RecordedAt = now.AddMinutes(-1),
            Status = OutboxStatus.Pending,
            RetryCount = retryCount,
            NextAttemptAt = now
        };
    }
}
