namespace Ecommerce.Payments.Infrastructure.DeadLetters;

/// <summary>
/// Settings for the dead-letter retry worker, bound from the <c>DeadLetterRetry</c>
/// configuration section (see <c>docs/dead-letter-retry.md</c>).
/// </summary>
public sealed class DeadLetterRetryOptions
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "DeadLetterRetry";

    /// <summary>Time between retry cycles, in milliseconds. Default 30 000.</summary>
    public int IntervalMs { get; set; } = 30_000;

    /// <summary>Maximum number of rows processed per cycle. Default 20.</summary>
    public int BatchSize { get; set; } = 20;

    /// <summary>Delay after the first failed retry, in milliseconds; doubles per retry. Default 5 000.</summary>
    public int BaseDelayMs { get; set; } = 5_000;

    /// <summary>Upper bound for the backoff delay, in milliseconds. Default 600 000 (10 minutes).</summary>
    public int MaxDelayMs { get; set; } = 600_000;

    /// <summary>Retry attempts after which a row is marked <c>Exhausted</c>. Default 8.</summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>
    /// Returns <see langword="true"/> when all values are positive and
    /// <see cref="MaxDelayMs"/> is at least <see cref="BaseDelayMs"/>.
    /// </summary>
    public bool IsValid() =>
        IntervalMs > 0 && BatchSize > 0 && BaseDelayMs > 0 && MaxRetries > 0 && MaxDelayMs >= BaseDelayMs;
}
