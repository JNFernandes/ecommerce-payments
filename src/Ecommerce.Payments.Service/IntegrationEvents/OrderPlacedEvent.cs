using System.Text.Json;
using System.Text.Json.Serialization;

namespace Ecommerce.Payments.Service.IntegrationEvents;

/// <summary>
/// Typed inbound integration event for the <c>OrderPlaced</c> message consumed from
/// <c>orders.order-placed</c>. Matches the actual payload published by the <c>orders</c>
/// service (<c>ecommerce-orders/src/domain/events/order-placed.event.ts</c>) — there is no
/// separate <c>orderId</c> field (the order id is <see cref="AggregateId"/>), no
/// <c>currency</c> field (the platform is currently single-currency; see
/// <see cref="DefaultCurrency"/>), and the charge amount is published as <c>totalAmount</c>,
/// not <c>amount</c>. See contracts/orders.order-placed.md for the full wire format.
/// </summary>
public sealed record OrderPlacedEvent
{
    /// <summary>The fixed currency assumed for all payments until the platform is multi-currency.</summary>
    public const string DefaultCurrency = "USD";

    /// <summary>Unique id of this event instance.</summary>
    [JsonPropertyName("eventId")]
    public Guid EventId { get; init; }

    /// <summary>When the order was placed.</summary>
    [JsonPropertyName("occurredAt")]
    public DateTimeOffset OccurredAt { get; init; }

    /// <summary>The order's aggregate id — also used as the idempotency key for <c>Payment</c>.</summary>
    [JsonPropertyName("aggregateId")]
    public Guid AggregateId { get; init; }

    /// <summary>Schema version of this event.</summary>
    [JsonPropertyName("version")]
    public int Version { get; init; }

    /// <summary>Customer being charged.</summary>
    [JsonPropertyName("customerId")]
    public Guid CustomerId { get; init; }

    /// <summary>Order total to charge. Must be greater than zero.</summary>
    [JsonPropertyName("totalAmount")]
    public decimal TotalAmount { get; init; }

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = false
    };

    /// <summary>
    /// Parses and validates a raw <c>OrderPlaced</c> JSON payload. Returns <see langword="false"/>
    /// with a diagnostic <paramref name="error"/> for malformed JSON, missing required fields, or
    /// a non-positive <see cref="TotalAmount"/> — this is the Consumer-level envelope validation
    /// boundary; invalid messages must never reach Domain.
    /// </summary>
    public static bool TryParse(string rawJson, out OrderPlacedEvent? orderPlaced, out string? error)
    {
        orderPlaced = null;
        error = null;

        OrderPlacedEvent? candidate;
        try
        {
            candidate = JsonSerializer.Deserialize<OrderPlacedEvent>(rawJson, SerializerOptions);
        }
        catch (JsonException ex)
        {
            error = $"Malformed OrderPlaced JSON: {ex.Message}";
            return false;
        }

        if (candidate is null)
        {
            error = "Malformed OrderPlaced JSON: message body was empty or null.";
            return false;
        }

        if (candidate.EventId == Guid.Empty)
        {
            error = "OrderPlaced.eventId is missing or invalid.";
            return false;
        }

        if (candidate.AggregateId == Guid.Empty)
        {
            error = "OrderPlaced.aggregateId is missing or invalid.";
            return false;
        }

        if (candidate.CustomerId == Guid.Empty)
        {
            error = "OrderPlaced.customerId is missing or invalid.";
            return false;
        }

        if (candidate.Version <= 0)
        {
            error = "OrderPlaced.version is missing or invalid.";
            return false;
        }

        if (candidate.OccurredAt == default)
        {
            error = "OrderPlaced.occurredAt is missing or invalid.";
            return false;
        }

        if (candidate.TotalAmount <= 0)
        {
            error = "OrderPlaced.totalAmount must be greater than zero.";
            return false;
        }

        orderPlaced = candidate;
        return true;
    }
}
