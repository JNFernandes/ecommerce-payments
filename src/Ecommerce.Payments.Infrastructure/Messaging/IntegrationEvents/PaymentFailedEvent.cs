using System.Text.Json.Serialization;

namespace Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents;

/// <summary>
/// Typed outbound integration event for the <c>PaymentFailed</c> message published to
/// <c>payments.payment-failed</c>. See contracts/payments.payment-failed.md for the wire
/// format.
/// </summary>
public sealed record PaymentFailedEvent
{
    /// <summary>Newly generated id for this published event instance.</summary>
    [JsonPropertyName("eventId")]
    public required Guid EventId { get; init; }

    /// <summary>When this event was raised (equal to <see cref="FailedAt"/>).</summary>
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

    /// <summary>Human-readable business reason the payment could not be completed.</summary>
    [JsonPropertyName("reason")]
    public required string Reason { get; init; }

    /// <summary>When the payment transitioned to <c>Failed</c>.</summary>
    [JsonPropertyName("failedAt")]
    public required DateTimeOffset FailedAt { get; init; }
}
