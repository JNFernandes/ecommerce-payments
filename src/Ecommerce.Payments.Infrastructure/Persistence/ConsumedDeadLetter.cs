namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>An inbound message to store in <c>consumed_dead_letters</c> (US-06).</summary>
/// <param name="Topic">Kafka topic it was consumed from.</param>
/// <param name="Partition">Kafka partition.</param>
/// <param name="Offset">Kafka offset within the partition.</param>
/// <param name="MessageKey">Key as received, or null.</param>
/// <param name="Payload">Value exactly as received, or null.</param>
/// <param name="Error">Why it could not be handled.</param>
/// <param name="Attempts">Failed handling attempts; 1 for a malformed message.</param>
public sealed record ConsumedDeadLetter(
    string Topic,
    int Partition,
    long Offset,
    string? MessageKey,
    string? Payload,
    string Error,
    int Attempts);
