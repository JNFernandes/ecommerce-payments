namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// The lifecycle state of a <see cref="Payment"/> aggregate.
/// Transitions: <see cref="Pending"/> → <see cref="Processed"/> | <see cref="Failed"/>;
/// <see cref="Processed"/> → <see cref="Refunded"/>.
/// </summary>
public enum PaymentStatus
{
    /// <summary>The payment has been created but not yet processed.</summary>
    Pending,

    /// <summary>The payment was successfully processed and the customer was charged.</summary>
    Processed,

    /// <summary>
    /// The payment could not be completed (e.g. the amount exceeded the configured threshold).
    /// Terminal: a failed payment is never processed or refunded.
    /// </summary>
    Failed,

    /// <summary>
    /// The full amount of a <see cref="Processed"/> payment was refunded because its order was
    /// cancelled. Terminal.
    /// </summary>
    Refunded
}
