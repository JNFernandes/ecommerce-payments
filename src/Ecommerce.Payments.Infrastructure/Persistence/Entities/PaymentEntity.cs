using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>payments</c> table. Kept separate from the
/// <see cref="Payment"/> domain aggregate per Constitution Principle I — the Repository maps
/// explicitly between the two so EF Core types never leak into the Domain layer.
/// </summary>
public sealed class PaymentEntity
{
    /// <summary>Payment id (the <c>aggregateId</c> of published payment events).</summary>
    public Guid Id { get; set; }

    /// <summary>The order this payment is for; unique.</summary>
    public Guid OrderId { get; set; }

    /// <summary>Customer being charged.</summary>
    public Guid CustomerId { get; set; }

    /// <summary>Charge amount.</summary>
    public decimal Amount { get; set; }

    /// <summary>ISO 4217 currency code.</summary>
    public string Currency { get; set; } = string.Empty;

    /// <summary>Lifecycle state, stored as a string.</summary>
    public PaymentStatus Status { get; set; }

    /// <summary><c>eventId</c> of the <c>OrderPlaced</c> that created the payment.</summary>
    public Guid SourceEventId { get; set; }

    /// <summary>When the payment was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>When the payment was processed; null unless Processed or Refunded.</summary>
    public DateTimeOffset? ProcessedAt { get; set; }

    /// <summary>Business reason for a failed payment.</summary>
    public string? FailureReason { get; set; }

    /// <summary>When the payment failed; null unless Failed.</summary>
    public DateTimeOffset? FailedAt { get; set; }

    /// <summary>When the payment was refunded; null unless <c>Status</c> is <c>Refunded</c>.</summary>
    public DateTimeOffset? RefundedAt { get; set; }

    /// <summary><c>eventId</c> of the <c>OrderCancelled</c> message that caused the refund.</summary>
    public Guid? RefundSourceEventId { get; set; }
}
