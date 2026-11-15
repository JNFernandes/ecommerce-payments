namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>Persistence settings, bound from the <c>Persistence</c> configuration section.</summary>
public sealed class PersistenceOptions
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "Persistence";

    /// <summary>
    /// Maximum time, in milliseconds, a transaction waits for the per-order lock
    /// (<see cref="OrderUnitOfWork"/>) before failing with a lock timeout, which is then retried
    /// like any transient database failure. Must be positive. Default 5000.
    /// </summary>
    public int OrderLockTimeoutMs { get; set; } = 5_000;
}
