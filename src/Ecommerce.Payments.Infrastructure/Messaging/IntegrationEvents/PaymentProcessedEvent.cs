using System.Text.Json.Serialization;

namespace Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents;

/// <summary>
/// Typed outbound integration event for the <c>PaymentProcessed</c> message published to
/// <c>payments.payment-processed</c>. See contracts/payments.payment-processed.md for the wire
/// format.
/// </summary>
public sealed record PaymentProcessedEvent
{
    /// <summary>Newly generated id for this published event instance.</summary>
    [JsonPropertyName("eventId")]
    public required Guid EventId { get; init; }

    /// <summary>When this event was raised (equal to <see cref="ProcessedAt"/>).</summary>
    [JsonPropertyName("occurredAt")]
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>The <c>Payment</c> aggregate id.</summary>
    [JsonPropertyName("aggregateId")]
    public required Guid AggregateId { get; init; }

    /// <summary>Schema version of this event.</summary>
    [JsonPropertyName("version")]
    public required int Version { get; init; }

    /// <summary>The order this payment was for.</summary>
    [JsonPropertyName("orderId")]
    public required Guid OrderId { get; init; }

    /// <summary>Amount charged.</summary>
    [JsonPropertyName("amount")]
    public required decimal Amount { get; init; }

    /// <summary>ISO 4217 currency code.</summary>
    [JsonPropertyName("currency")]
    public required string Currency { get; init; }

    /// <summary>When the payment transitioned to <c>Processed</c>.</summary>
    [JsonPropertyName("processedAt")]
    public required DateTimeOffset ProcessedAt { get; init; }
}
