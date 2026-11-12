namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Outcome of <see cref="Payment.RequestRefund"/>: either the payment was refunded
/// (<see cref="Refunded"/>) or the request was ignored for a business reason
/// (<see cref="Ignored"/>). Ignoring is an expected outcome, not an error (Constitution VI
/// "Refund rules"), so it is returned rather than thrown.
/// </summary>
public abstract record RefundDecision
{
    private RefundDecision()
    {
    }

    /// <summary>The payment was refunded; <paramref name="Event"/> must be published after the save.</summary>
    /// <param name="Event">The resulting domain event.</param>
    public sealed record Refunded(PaymentRefunded Event) : RefundDecision;

    /// <summary>The payment was not refunded; nothing changed.</summary>
    /// <param name="Reason">Why the refund was not applied.</param>
    public sealed record Ignored(RefundIgnoredReason Reason) : RefundDecision;
}
