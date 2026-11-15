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
    /// Validates and dispatches a single raw message to the Service layer. Returns true (commit)
    /// when it was fully processed. Throws <see cref="InvalidMessageException"/> for a malformed
    /// envelope (the base stores it in <c>consumed_dead_letters</c> and commits, US-06), and
    /// rethrows when a save failure survived the handler's retries, so the base retries it.
    /// </summary>
    internal override async Task<bool> HandleMessageAsync(string rawMessage, CancellationToken cancellationToken)
    {
        if (!OrderPlacedEvent.TryParse(rawMessage, out var orderPlaced, out var error))
        {
            // Malformed envelopes never reach Domain and are never retried.
            throw new InvalidMessageException(error!);
        }

        using var scope = ScopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<ProcessPaymentHandler>();

        await handler.HandleAsync(orderPlaced!, cancellationToken);
        return true;
    }
}
