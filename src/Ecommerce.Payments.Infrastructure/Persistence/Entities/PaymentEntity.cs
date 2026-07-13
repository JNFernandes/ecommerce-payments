using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Infrastructure.Persistence.Entities;

/// <summary>
/// EF Core persistence model for the <c>payments</c> table. Kept separate from the
/// <see cref="Payment"/> domain aggregate per Constitution Principle I — the Repository maps
/// explicitly between the two so EF Core types never leak into the Domain layer.
/// </summary>
public sealed class PaymentEntity
{
    public Guid Id { get; set; }

    public Guid OrderId { get; set; }

    public Guid CustomerId { get; set; }

    public decimal Amount { get; set; }

    public string Currency { get; set; } = string.Empty;

    public PaymentStatus Status { get; set; }

    public Guid SourceEventId { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset? ProcessedAt { get; set; }

    public string? FailureReason { get; set; }

    public DateTimeOffset? FailedAt { get; set; }
}
