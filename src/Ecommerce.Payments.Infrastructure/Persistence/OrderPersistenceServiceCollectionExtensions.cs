using Ecommerce.Payments.Service.Orders;
using Microsoft.Extensions.DependencyInjection;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>DI registration for the per-order unit of work and cancellation markers (US-05).</summary>
public static class OrderPersistenceServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IOrderUnitOfWork"/> and <see cref="ICancellationMarkerRepository"/>
    /// (scoped, over the scoped <see cref="PaymentsDbContext"/>) and ensures
    /// <see cref="PersistenceOptions"/> is available. The host binds and validates the
    /// <c>Persistence</c> section separately; without it the defaults apply.
    /// </summary>
    public static IServiceCollection AddOrderPersistence(this IServiceCollection services)
    {
        services.AddOptions<PersistenceOptions>();
        services.AddScoped<IOrderUnitOfWork, OrderUnitOfWork>();
        services.AddScoped<ICancellationMarkerRepository, CancellationMarkerRepository>();
        return services;
    }
}
