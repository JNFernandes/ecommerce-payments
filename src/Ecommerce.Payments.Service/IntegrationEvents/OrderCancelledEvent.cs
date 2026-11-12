using System.Text.Json;
using System.Text.Json.Serialization;

namespace Ecommerce.Payments.Service.IntegrationEvents;

/// <summary>
/// Typed inbound integration event for the <c>OrderCancelled</c> message consumed from
/// <c>orders.order-cancelled</c>. Matches the payload published by the <c>orders</c> service
/// (<c>ecommerce-orders/src/domain/events/order-cancelled.event.ts</c>); see
/// <c>specs/004-refund-on-order-cancelled/contracts/orders.order-cancelled.md</c>. The order is
/// identified by <see cref="AggregateId"/>; there is no separate <c>orderId</c> field.
/// </summary>
public sealed record OrderCancelledEvent
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = false
    };

    /// <summary>
    /// Unique id of this publish attempt (the orders service issues a new one per publish).
    /// Stored on the refunded payment and used to recognise redeliveries.
    /// </summary>
    [JsonPropertyName("eventId")]
    public Guid EventId { get; init; }

    /// <summary>When the order was cancelled.</summary>
    [JsonPropertyName("occurredAt")]
    public DateTimeOffset OccurredAt { get; init; }

    /// <summary>The order id — used to find the payment to refund.</summary>
    [JsonPropertyName("aggregateId")]
    public Guid AggregateId { get; init; }

    /// <summary>Schema version of this event (1).</summary>
    [JsonPropertyName("version")]
    public int Version { get; init; }

    /// <summary>The customer whose order was cancelled. Optional; not used by Payments.</summary>
    [JsonPropertyName("customerId")]
    public Guid? CustomerId { get; init; }

    /// <summary>Why the order was cancelled, when the orders service supplied one. Logged only.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; init; }

    /// <summary>
    /// Parses and validates a raw <c>OrderCancelled</c> JSON payload — the Consumer-level
    /// envelope validation boundary. Returns <see langword="false"/> with a field-specific
    /// <paramref name="error"/> for malformed JSON or a missing/invalid <c>eventId</c>,
    /// <c>aggregateId</c>, <c>occurredAt</c> or <c>version</c>. Optional fields and unknown fields
    /// never cause a rejection.
    /// </summary>
    public static bool TryParse(string rawJson, out OrderCancelledEvent? orderCancelled, out string? error)
    {
        orderCancelled = null;
        error = null;

        OrderCancelledEvent? candidate;
        try
        {
            candidate = JsonSerializer.Deserialize<OrderCancelledEvent>(rawJson, SerializerOptions);
        }
        catch (JsonException ex)
        {
            error = $"Malformed OrderCancelled JSON: {ex.Message}";
            return false;
        }

        if (candidate is null)
        {
            error = "Malformed OrderCancelled JSON: message body was empty or null.";
            return false;
        }

        if (candidate.EventId == Guid.Empty)
        {
            error = "OrderCancelled.eventId is missing or invalid.";
            return false;
        }

        if (candidate.AggregateId == Guid.Empty)
        {
            error = "OrderCancelled.aggregateId is missing or invalid.";
            return false;
        }

        if (candidate.Version <= 0)
        {
            error = "OrderCancelled.version is missing or invalid.";
            return false;
        }

        if (candidate.OccurredAt == default)
        {
            error = "OrderCancelled.occurredAt is missing or invalid.";
            return false;
        }

        orderCancelled = candidate;
        return true;
    }
}
