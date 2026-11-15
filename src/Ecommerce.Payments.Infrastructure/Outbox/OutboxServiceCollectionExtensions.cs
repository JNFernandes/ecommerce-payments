using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>DI registration for the transactional outbox writer (US-07).</summary>
public static class OutboxServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IPaymentOutbox"/> (scoped, over the scoped <c>PaymentsDbContext</c>;
    /// needs <c>KafkaOptions</c>; adds <see cref="TimeProvider.System"/> if no clock is registered). The relay
    /// (<see cref="OutboxRelayWorker"/>) is registered separately by the host.
    /// </summary>
    public static IServiceCollection AddPaymentOutbox(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddScoped<IPaymentOutbox, PaymentOutbox>();
        return services;
    }
}
