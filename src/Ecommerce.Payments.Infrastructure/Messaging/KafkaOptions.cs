namespace Ecommerce.Payments.Infrastructure.Messaging;

/// <summary>
/// Kafka connection and topic configuration, bound from the <c>Kafka</c> configuration section.
/// Shared by the Consumer (subscribes to <see cref="OrderPlacedTopic"/>) and the Publisher
/// (publishes to <see cref="PaymentProcessedTopic"/> or <see cref="PaymentFailedTopic"/>).
/// </summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string ConsumerGroupId { get; set; } = string.Empty;

    public string OrderPlacedTopic { get; set; } = string.Empty;

    public string PaymentProcessedTopic { get; set; } = string.Empty;

    public string PaymentFailedTopic { get; set; } = string.Empty;
}
