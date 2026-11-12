namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>payment_dead_letters</c> table. Holds integration events
/// that failed to publish after a successful <c>Payment</c> save (Constitution Principle II),
/// together with the retry bookkeeping used by the dead-letter retry worker (US-03, ADR 0003).
/// The original event fields (<see cref="Payload"/>, <see cref="Topic"/>,
/// <see cref="MessageKey"/>, <see cref="EventType"/>) never change after creation, so a retry
/// republishes exactly the original message with its original <c>eventId</c>.
/// </summary>
public sealed class PaymentDeadLetterEntity
{
    /// <summary>Primary key of the dead-letter row.</summary>
    public Guid Id { get; set; }

    /// <summary>The <c>Payment</c> the event belongs to.</summary>
    public Guid PaymentId { get; set; }

    /// <summary>Integration event type name, e.g. <c>PaymentProcessedEvent</c>.</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>The serialized integration event that failed to publish. Republished unchanged.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>Kafka topic the event was originally published to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Kafka message key of the original publish (the payment id).</summary>
    public string MessageKey { get; set; } = string.Empty;

    /// <summary>Error message of the original failed publish.</summary>
    public string FailureReason { get; set; } = string.Empty;

    /// <summary>When the dead-letter row was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>Current retry state.</summary>
    public DeadLetterStatus Status { get; set; } = DeadLetterStatus.PendingRetry;

    /// <summary>Number of retry attempts made by the worker (the original publish is not counted).</summary>
    public int RetryCount { get; set; }

    /// <summary>When the row is next due for a retry; only meaningful while <see cref="DeadLetterStatus.PendingRetry"/>.</summary>
    public DateTimeOffset? NextRetryAt { get; set; }

    /// <summary>When the worker last attempted to republish the row.</summary>
    public DateTimeOffset? LastAttemptAt { get; set; }

    /// <summary>Error message of the most recent failed retry attempt.</summary>
    public string? LastError { get; set; }

    /// <summary>When a retry succeeded and the row became <see cref="DeadLetterStatus.Resolved"/>.</summary>
    public DateTimeOffset? ResolvedAt { get; set; }
}
