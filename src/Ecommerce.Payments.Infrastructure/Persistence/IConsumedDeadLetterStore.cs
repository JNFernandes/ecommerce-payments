namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Stores inbound messages that could not be handled in <c>consumed_dead_letters</c>
/// (US-06, ADR 0005). Used by the Consumer layer's <c>KafkaConsumerBase</c>, which commits the
/// message's offset only after <see cref="StoreAsync"/> returns.
/// </summary>
public interface IConsumedDeadLetterStore
{
    /// <summary>
    /// Durably stores <paramref name="deadLetter"/>. Idempotent per Kafka position (topic,
    /// partition, offset): storing the same position again keeps the first row and succeeds.
    /// Throws when the row could not be stored; the caller must then not commit the offset.
    /// </summary>
    public Task StoreAsync(ConsumedDeadLetter deadLetter, CancellationToken cancellationToken);
}
