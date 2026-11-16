using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ecommerce.Payments.Infrastructure.Retention;

/// <summary>DI registration for data retention (US-09).</summary>
public static class RetentionServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IRetentionPurger"/> (scoped, over the scoped <c>PaymentsDbContext</c>)
    /// and <see cref="TimeProvider.System"/> if no clock is registered. The host binds and
    /// validates <see cref="RetentionOptions"/> and registers <see cref="RetentionWorker"/>.
    /// </summary>
    public static IServiceCollection AddRetention(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddScoped<IRetentionPurger, RetentionPurger>();
        return services;
    }
}
