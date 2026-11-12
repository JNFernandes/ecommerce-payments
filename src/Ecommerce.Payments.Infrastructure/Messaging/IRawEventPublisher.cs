namespace Ecommerce.Payments.Infrastructure.Messaging;

/// <summary>
/// Low-level Kafka publish of an already-serialized message. Used by the normal publish path
/// (which dead-letters on failure) and by the dead-letter retry worker (which updates the
/// existing dead-letter row on failure).
/// </summary>
public interface IRawEventPublisher
{
    /// <summary>
    /// Publishes <paramref name="payload"/> to <paramref name="topic"/> with
    /// <paramref name="key"/>, unchanged. Throws on failure and NEVER writes a dead-letter row —
    /// failure handling is the caller's responsibility.
    /// </summary>
    /// <param name="topic">Destination topic.</param>
    /// <param name="key">Kafka message key.</param>
    /// <param name="payload">Serialized JSON message value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="Confluent.Kafka.ProduceException{TKey, TValue}">The broker rejected or timed out the message.</exception>
    public Task PublishRawAsync(string topic, string key, string payload, CancellationToken cancellationToken);
}
