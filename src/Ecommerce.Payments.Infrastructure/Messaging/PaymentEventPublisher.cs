using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Messaging;

/// <summary>
/// Confluent.Kafka implementation of <see cref="IRawEventPublisher"/>: the Publisher layer
/// (Constitution v1.3.0, Principle I). It publishes stored outbox entries unchanged and is
/// invoked only by the outbox relay (<c>OutboxRelayProcessor</c>, US-07, ADR 0006). Serialization
/// happens earlier, when the entry is recorded (<c>PaymentOutbox</c>), so what is published is
/// exactly what was stored. A singleton: the producer is long-lived and reused.
/// </summary>
public sealed class PaymentEventPublisher : IRawEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private int _disposed;

    /// <summary>Builds the producer (no broker connection is made here).</summary>
    public PaymentEventPublisher(IOptions<KafkaOptions> kafkaOptions)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            // Bound how long a publish attempt can hang before the relay records the failure and
            // backs off — the librdkafka default (5 minutes) would hold the outbox row lock that long.
            MessageTimeoutMs = 10_000
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    /// <inheritdoc />
    public async Task PublishRawAsync(string topic, string key, string payload, CancellationToken cancellationToken)
    {
        var message = new Message<string, string> { Key = key, Value = payload };
        await _producer.ProduceAsync(topic, message, cancellationToken);
    }

    /// <summary>
    /// Flushes pending messages (up to 10 s) and disposes the producer. Idempotent: the single
    /// instance may be registered under several service types, and the DI container disposes it
    /// once per registration.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
