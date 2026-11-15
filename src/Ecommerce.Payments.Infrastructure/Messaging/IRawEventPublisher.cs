namespace Ecommerce.Payments.Infrastructure.Messaging;

/// <summary>
/// Kafka publish of an already-serialized message — the only way this service publishes. Used
/// solely by the outbox relay, which publishes stored <c>payment_outbox</c> entries and records
/// failures on the entry itself (US-07, ADR 0006).
/// </summary>
public interface IRawEventPublisher
{
    /// <summary>
    /// Publishes <paramref name="payload"/> to <paramref name="topic"/> with
    /// <paramref name="key"/>, unchanged. Throws on failure; failure handling is the caller's
    /// responsibility.
    /// </summary>
    /// <param name="topic">Destination topic.</param>
    /// <param name="key">Kafka message key.</param>
    /// <param name="payload">Serialized JSON message value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="Confluent.Kafka.ProduceException{TKey, TValue}">The broker rejected or timed out the message.</exception>
    public Task PublishRawAsync(string topic, string key, string payload, CancellationToken cancellationToken);
}
