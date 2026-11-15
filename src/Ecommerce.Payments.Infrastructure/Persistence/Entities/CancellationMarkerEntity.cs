using Ecommerce.Payments.Domain.Orders;

namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>order_cancellation_markers</c> table, kept separate from
/// the <see cref="CancellationMarker"/> domain type (Constitution Principle I).
/// </summary>
public sealed class CancellationMarkerEntity
{
    /// <summary>The cancelled order; primary key, so at most one marker per order.</summary>
    public Guid OrderId { get; set; }

    /// <summary><c>eventId</c> of the <c>OrderCancelled</c> that created the marker.</summary>
    public Guid SourceEventId { get; set; }

    /// <summary>When the order was cancelled (<c>OrderCancelled.occurredAt</c>).</summary>
    public DateTimeOffset CancelledAt { get; set; }

    /// <summary>When Payments recorded the marker.</summary>
    public DateTimeOffset RecordedAt { get; set; }

    /// <summary>Optional cancellation reason.</summary>
    public string? Reason { get; set; }
}
