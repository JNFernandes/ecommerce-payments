namespace Ecommerce.Payments.Domain.Orders;

/// <summary>
/// Records that an order was cancelled before Payments saw its <c>OrderPlaced</c> (Constitution
/// VI "Cancellation markers"). <c>OrderPlaced</c> and <c>OrderCancelled</c> travel on different
/// topics with no relative ordering, so a cancellation can arrive first; a later
/// <c>OrderPlaced</c> for a marked order is never charged. Immutable: created once, never
/// changed or removed.
/// </summary>
public sealed class CancellationMarker
{
    private CancellationMarker(
        Guid orderId,
        Guid sourceEventId,
        DateTimeOffset cancelledAt,
        DateTimeOffset recordedAt,
        string? reason)
    {
        OrderId = orderId;
        SourceEventId = sourceEventId;
        CancelledAt = cancelledAt;
        RecordedAt = recordedAt;
        Reason = reason;
    }

    /// <summary>The cancelled order (the <c>OrderCancelled</c> <c>aggregateId</c>); one marker per order.</summary>
    public Guid OrderId { get; }

    /// <summary><c>eventId</c> of the <c>OrderCancelled</c> that created the marker; used to detect redeliveries.</summary>
    public Guid SourceEventId { get; }

    /// <summary>When the order was cancelled (the <c>OrderCancelled</c> <c>occurredAt</c>).</summary>
    public DateTimeOffset CancelledAt { get; }

    /// <summary>When Payments recorded the marker.</summary>
    public DateTimeOffset RecordedAt { get; }

    /// <summary>Optional cancellation reason from the <c>OrderCancelled</c> event.</summary>
    public string? Reason { get; }

    /// <summary>
    /// Creates a marker for an order cancelled before any payment exists.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="orderId"/> or <paramref name="sourceEventId"/> is empty.</exception>
    public static CancellationMarker Record(
        Guid orderId,
        Guid sourceEventId,
        DateTimeOffset cancelledAt,
        DateTimeOffset recordedAt,
        string? reason)
    {
        if (orderId == Guid.Empty)
        {
            throw new ArgumentException("Order id must not be empty.", nameof(orderId));
        }

        if (sourceEventId == Guid.Empty)
        {
            throw new ArgumentException("Source event id must not be empty.", nameof(sourceEventId));
        }

        return new CancellationMarker(orderId, sourceEventId, cancelledAt, recordedAt, reason);
    }

    /// <summary>Persistence only — restores a saved marker without running validation.</summary>
    public static CancellationMarker Rehydrate(
        Guid orderId,
        Guid sourceEventId,
        DateTimeOffset cancelledAt,
        DateTimeOffset recordedAt,
        string? reason) =>
        new(orderId, sourceEventId, cancelledAt, recordedAt, reason);
}
