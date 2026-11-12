namespace Ecommerce.Payments.Infrastructure.Messaging;

/// <summary>
/// Kafka connection and topic configuration, bound from the <c>Kafka</c> configuration section.
/// Shared by the consumers (subscribe to <see cref="OrderPlacedTopic"/> and
/// <see cref="OrderCancelledTopic"/>) and the Publisher (publishes to
/// <see cref="PaymentProcessedTopic"/>, <see cref="PaymentFailedTopic"/> and
/// <see cref="PaymentRefundedTopic"/>).
/// </summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string ConsumerGroupId { get; set; } = string.Empty;

    public string OrderPlacedTopic { get; set; } = string.Empty;

    public string PaymentProcessedTopic { get; set; } = string.Empty;

    public string PaymentFailedTopic { get; set; } = string.Empty;

    /// <summary>Inbound topic for <c>OrderCancelled</c> (default <c>orders.order-cancelled</c>).</summary>
    public string OrderCancelledTopic { get; set; } = "orders.order-cancelled";

    /// <summary>Outbound topic for <c>PaymentRefunded</c> (default <c>payments.payment-refunded</c>).</summary>
    public string PaymentRefundedTopic { get; set; } = "payments.payment-refunded";

    /// <summary>
    /// First wait, in milliseconds, before a consumer re-consumes a message whose handling failed.
    /// Doubles per consecutive failure up to <see cref="ConsumerRetryMaxDelayMs"/>. Default 1000.
    /// </summary>
    public int ConsumerRetryBaseDelayMs { get; set; } = 1_000;

    /// <summary>Upper bound, in milliseconds, for the consumer retry wait. Default 60 000.</summary>
    public int ConsumerRetryMaxDelayMs { get; set; } = 60_000;
}
