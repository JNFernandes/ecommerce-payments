using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>DI registration for applying migrations at startup.</summary>
public static class DatabaseMigrationServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="DatabaseMigrator"/> as a hosted service, and
    /// <see cref="TimeProvider.System"/> if no clock is registered. Call it <b>before</b>
    /// registering any other hosted service: the host starts them in registration order, so this
    /// guarantees the schema is migrated before anything consumes or publishes. The host binds
    /// and validates <see cref="DatabaseMigrationOptions"/>; without it the defaults apply.
    /// </summary>
    public static IServiceCollection AddDatabaseMigration(this IServiceCollection services)
    {
        services.AddOptions<DatabaseMigrationOptions>();
        services.TryAddSingleton(TimeProvider.System);
        services.AddHostedService<DatabaseMigrator>();
        return services;
    }
}
