namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Startup migration settings, bound from the <c>DatabaseMigration</c> configuration section
/// (see <see cref="DatabaseMigrator"/>).
/// </summary>
public sealed class DatabaseMigrationOptions
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "DatabaseMigration";

    /// <summary>
    /// Whether pending EF Core migrations are applied when the worker starts. Default <c>true</c>.
    /// Set to <c>false</c> when migrations are applied by a separate step (e.g. a deploy job).
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Attempts made while the database is unreachable before startup fails. Must be positive.
    /// Default 10.
    /// </summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>
    /// Wait, in milliseconds, between attempts while the database is unreachable. Must be
    /// positive. Default 3000.
    /// </summary>
    public int RetryDelayMs { get; set; } = 3_000;

    /// <summary>Whether the settings are usable.</summary>
    public bool IsValid() => MaxAttempts > 0 && RetryDelayMs > 0;
}
