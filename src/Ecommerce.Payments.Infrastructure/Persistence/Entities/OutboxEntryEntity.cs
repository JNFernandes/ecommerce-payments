namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>payment_outbox</c> table (US-07, ADR 0006): one outbound
/// payment event, recorded in the same transaction as the payment change that produced it and
/// published by the outbox relay. The event fields (<see cref="EventId"/>, <see cref="Payload"/>,
/// <see cref="Topic"/>, <see cref="MessageKey"/>, <see cref="EventType"/>) never change after
/// creation, so every publish attempt sends exactly the recorded message with its original
/// <c>eventId</c>. Entries are kept after publishing for support.
/// </summary>
public sealed class OutboxEntryEntity
{
    /// <summary>Primary key of the entry.</summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Recording order (database identity). Entries for the same payment are published in
    /// ascending <see cref="Sequence"/>.
    /// </summary>
    public long Sequence { get; set; }

    /// <summary>The <c>eventId</c> of the serialized integration event; unique.</summary>
    public Guid EventId { get; set; }

    /// <summary>The <c>Payment</c> the event belongs to (also the Kafka key).</summary>
    public Guid PaymentId { get; set; }

    /// <summary>Integration event type name, e.g. <c>PaymentProcessedEvent</c>.</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>The serialized integration event. Published unchanged.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>Kafka topic to publish to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Kafka message key (the payment id).</summary>
    public string MessageKey { get; set; } = string.Empty;

    /// <summary>When the entry was recorded (with the payment change).</summary>
    public DateTimeOffset RecordedAt { get; set; }

    /// <summary>Delivery state.</summary>
    public OutboxStatus Status { get; set; } = OutboxStatus.Pending;

    /// <summary>Number of failed publish attempts.</summary>
    public int RetryCount { get; set; }

    /// <summary>When the entry is next due for a publish attempt; only meaningful while <see cref="OutboxStatus.Pending"/>.</summary>
    public DateTimeOffset NextAttemptAt { get; set; }

    /// <summary>When the relay last attempted to publish the entry.</summary>
    public DateTimeOffset? LastAttemptAt { get; set; }

    /// <summary>Error message of the most recent failed publish attempt.</summary>
    public string? LastError { get; set; }

    /// <summary>When the entry was published and became <see cref="OutboxStatus.Published"/>.</summary>
    public DateTimeOffset? PublishedAt { get; set; }
}
