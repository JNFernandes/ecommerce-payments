using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Consumer.Consumers;

/// <summary>
/// Consumer for <c>orders.order-cancelled</c> (US-04). Deserializes each message into a typed
/// <see cref="OrderCancelledEvent"/>, rejects malformed envelopes before they reach the Service
/// layer, and hands valid ones to <see cref="RefundPaymentHandler"/>. The loop,
/// commit-after-success and retry-in-place behaviour live in <see cref="KafkaConsumerBase"/>.
/// </summary>
public sealed class OrderCancelledConsumer : KafkaConsumerBase
{
    /// <summary>Creates the consumer.</summary>
    public OrderCancelledConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<OrderCancelledConsumer> logger)
        : base(kafkaOptions, scopeFactory, logger)
    {
    }

    /// <inheritdoc />
    protected override string Topic => KafkaOptions.OrderCancelledTopic;

    /// <summary>
    /// Validates and dispatches one raw <c>OrderCancelled</c> message. Returns true (commit) when
    /// the envelope was rejected or the cancellation was fully handled — including when no refund
    /// was needed. Throws when the database stayed unavailable, so the base retries the message.
    /// </summary>
    internal override async Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken)
    {
        if (!OrderCancelledEvent.TryParse(rawMessage, out var orderCancelled, out var error))
        {
            Logger.LogWarning(
                "Rejecting malformed OrderCancelled message: {Error}. Raw message logged for review.",
                error);
            // Malformed envelopes never reach Domain; commit so the poison message is not
            // redelivered forever.
            return true;
        }

        using var scope = ScopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<RefundPaymentHandler>();

        await handler.HandleAsync(orderCancelled!, cancellationToken);
        return true;
    }
}
