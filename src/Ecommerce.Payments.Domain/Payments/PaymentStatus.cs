namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// The lifecycle state of a <see cref="Payment"/> aggregate.
/// </summary>
public enum PaymentStatus
{
    /// <summary>The payment has been created but not yet processed.</summary>
    Pending,

    /// <summary>The payment was successfully processed and the customer was charged.</summary>
    Processed,

    /// <summary>
    /// The payment could not be processed. Reserved for a future story (payment failure
    /// handling); no flow in this feature transitions a <see cref="Payment"/> into this state.
    /// </summary>
    Failed
}
