namespace Ecommerce.Payments.Consumer.Consumers;

/// <summary>
/// Thrown by a consumer's <c>HandleMessageAsync</c> for an envelope that can never be handled
/// (unreadable, or missing/invalid required fields). <see cref="KafkaConsumerBase"/> stores such
/// a message in <c>consumed_dead_letters</c> with attempts 1 and commits it, without retrying
/// (US-06, ADR 0005).
/// </summary>
public sealed class InvalidMessageException : Exception
{
    /// <summary>Creates the exception with the validation error.</summary>
    public InvalidMessageException(string reason)
        : base($"Malformed message: {reason}")
    {
        Reason = reason;
    }

    /// <summary>The validation error, e.g. "missing eventId".</summary>
    public string Reason { get; }
}
