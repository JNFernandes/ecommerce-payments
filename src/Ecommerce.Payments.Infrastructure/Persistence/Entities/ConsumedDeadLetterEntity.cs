namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>consumed_dead_letters</c> table (US-06, ADR 0005): an
/// inbound Kafka message Payments could not handle — malformed, or still failing after
/// <c>Kafka:ConsumerMaxAttempts</c> attempts — kept exactly as received so it can be inspected and
/// replayed. At most one row per Kafka position; rows are insert-only.
/// </summary>
public sealed class ConsumedDeadLetterEntity
{
    /// <summary>Primary key.</summary>
    public Guid Id { get; set; }

    /// <summary>Kafka topic the message was consumed from.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Kafka partition of the message.</summary>
    public int Partition { get; set; }

    /// <summary>Kafka offset of the message within its partition.</summary>
    public long Offset { get; set; }

    /// <summary>Message key as received (UTF-8 text), or null when the message had none.</summary>
    public string? MessageKey { get; set; }

    /// <summary>Message value exactly as received, even if it is not JSON; null for an empty (tombstone) value.</summary>
    public string? Payload { get; set; }

    /// <summary>Why it failed: exception type and message, plus the innermost exception when different.</summary>
    public string Error { get; set; } = string.Empty;

    /// <summary>Failed handling attempts before it was stored; 1 for a malformed message.</summary>
    public int Attempts { get; set; }

    /// <summary>When the message was stored.</summary>
    public DateTimeOffset FailedAt { get; set; }
}
