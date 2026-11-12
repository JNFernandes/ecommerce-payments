using System.Text.Json.Serialization;

namespace Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents;

/// <summary>
/// Typed outbound integration event for the <c>PaymentRefunded</c> message published to
/// <c>payments.payment-refunded</c>, keyed by the payment id. Published only after the refund
/// is durably saved. See
/// <c>specs/004-refund-on-order-cancelled/contracts/payments.payment-refunded.md</c>.
/// </summary>
public sealed record PaymentRefundedEvent
{
    /// <summary>Newly generated id for this event; unchanged if the dead-letter worker republishes it.</summary>
    [JsonPropertyName("eventId")]
    public required Guid EventId { get; init; }

    /// <summary>When this event was raised (equal to <see cref="RefundedAt"/>).</summary>
    [JsonPropertyName("occurredAt")]
    public required DateTimeOffset OccurredAt { get; init; }

    /// <summary>The <c>Payment</c> aggregate id (also the Kafka message key).</summary>
    [JsonPropertyName("aggregateId")]
    public required Guid AggregateId { get; init; }

    /// <summary>Schema version of this event (1).</summary>
    [JsonPropertyName("version")]
    public required int Version { get; init; }

    /// <summary>The order the refunded payment was for. Downstream consumers identify the order by this field.</summary>
    [JsonPropertyName("orderId")]
    public required Guid OrderId { get; init; }

    /// <summary>Amount refunded — the full amount of the payment.</summary>
    [JsonPropertyName("amount")]
    public required decimal Amount { get; init; }

    /// <summary>ISO 4217 currency code.</summary>
    [JsonPropertyName("currency")]
    public required string Currency { get; init; }

    /// <summary>When the payment transitioned to <c>Refunded</c>.</summary>
    [JsonPropertyName("refundedAt")]
    public required DateTimeOffset RefundedAt { get; init; }
}
