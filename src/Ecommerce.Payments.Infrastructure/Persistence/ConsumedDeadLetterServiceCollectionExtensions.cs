using Microsoft.Extensions.DependencyInjection;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>DI registration for the inbound dead-letter store (US-06).</summary>
public static class ConsumedDeadLetterServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IConsumedDeadLetterStore"/> (scoped, over the scoped
    /// <see cref="PaymentsDbContext"/>; needs a <see cref="TimeProvider"/> registration).
    /// </summary>
    public static IServiceCollection AddConsumedDeadLetterStore(this IServiceCollection services)
    {
        services.AddScoped<IConsumedDeadLetterStore, ConsumedDeadLetterStore>();
        return services;
    }
}
