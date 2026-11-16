namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Thrown by <see cref="DatabaseMigrator"/> when the schema could not be migrated at startup:
/// the database stayed unreachable for every attempt, or a migration failed. It has already been
/// logged as Critical; the host stops before any consumer starts.
/// </summary>
public sealed class DatabaseMigrationException : Exception
{
    /// <summary>Creates the exception.</summary>
    public DatabaseMigrationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
