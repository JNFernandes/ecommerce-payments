namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Thrown when a <see cref="Payment"/> state transition is requested that is not valid from the
/// aggregate's current <see cref="PaymentStatus"/> (e.g. processing an already-processed payment).
/// </summary>
public sealed class InvalidPaymentTransitionException : Exception
{
    public InvalidPaymentTransitionException(string message)
        : base(message)
    {
    }
}
