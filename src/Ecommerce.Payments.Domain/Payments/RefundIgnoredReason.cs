namespace Ecommerce.Payments.Domain.Payments;

/// <summary>Why <see cref="Payment.RequestRefund"/> decided not to refund a payment.</summary>
public enum RefundIgnoredReason
{
    /// <summary>
    /// The payment never completed (<see cref="PaymentStatus.Failed"/> or
    /// <see cref="PaymentStatus.Pending"/>), so the customer was never charged.
    /// </summary>
    NotCompleted,

    /// <summary>The payment is already <see cref="PaymentStatus.Refunded"/>.</summary>
    AlreadyRefunded
}
