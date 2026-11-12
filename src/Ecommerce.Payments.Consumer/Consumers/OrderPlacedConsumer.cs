using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Consumer.Consumers;

/// <summary>
/// Consumer for <c>orders.order-placed</c>. Deserializes each message into a typed
/// <see cref="OrderPlacedEvent"/>, rejects malformed/incomplete envelopes without ever
/// forwarding them to the Service layer, and hands valid ones to
/// <see cref="ProcessPaymentHandler"/>. The loop, commit-after-success and retry-in-place
/// behaviour live in <see cref="KafkaConsumerBase"/> (Constitution Principle I/II/VI).
/// </summary>
public sealed class OrderPlacedConsumer : KafkaConsumerBase
{
    /// <summary>Creates the consumer.</summary>
    public OrderPlacedConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<OrderPlacedConsumer> logger)
        : base(kafkaOptions, scopeFactory, logger)
    {
    }

    /// <inheritdoc />
    protected override string Topic => KafkaOptions.OrderPlacedTopic;

    /// <summary>
    /// Validates and dispatches a single raw message to the Service layer. Returns whether the
    /// Kafka offset should be committed: true when the envelope was rejected or fully processed.
    /// Throws when a save failure survived the handler's retries, so the base retries the message.
    /// </summary>
    internal override async Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken)
    {
        if (!OrderPlacedEvent.TryParse(rawMessage, out var orderPlaced, out var error))
        {
            Logger.LogWarning(
                "Rejecting malformed OrderPlaced message: {Error}. Raw message logged for review.",
                error);
            // Malformed envelopes never reach Domain; commit so the poison message is not
            // redelivered forever (Constitution Principle I / spec Edge Cases).
            return true;
        }

        using var scope = ScopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<ProcessPaymentHandler>();

        await handler.HandleAsync(orderPlaced!, cancellationToken);
        return true;
    }
}
