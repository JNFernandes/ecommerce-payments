using Ecommerce.Payments.Infrastructure.Persistence.Entities;

namespace Ecommerce.Payments.Infrastructure.Tests.DeadLetters;

/// <summary>Builds dead-letter rows for unit tests.</summary>
internal static class DeadLetterRows
{
    public static PaymentDeadLetterEntity Pending(DateTimeOffset now, int retryCount = 0)
    {
        var paymentId = Guid.NewGuid();
        return new PaymentDeadLetterEntity
        {
            Id = Guid.NewGuid(),
            PaymentId = paymentId,
            EventType = "PaymentProcessedEvent",
            Payload = $$"""{"eventId":"{{Guid.NewGuid()}}","aggregateId":"{{paymentId}}"}""",
            Topic = "payments.payment-processed",
            MessageKey = paymentId.ToString(),
            FailureReason = "Local: Message timed out",
            CreatedAt = now.AddMinutes(-1),
            Status = DeadLetterStatus.PendingRetry,
            RetryCount = retryCount,
            NextRetryAt = now
        };
    }
}
