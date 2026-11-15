namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>
/// Settings for the outbox relay (<see cref="OutboxRelayWorker"/>), bound from the
/// <c>OutboxRelay</c> configuration section (US-07, ADR 0006; see <c>docs/outbox.md</c>).
/// Validated at startup with <see cref="IsValid"/>.
/// </summary>
public sealed class OutboxRelayOptions
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "OutboxRelay";

    /// <summary>
    /// Time between relay cycles, in milliseconds. Default 250, so an entry is normally visible
    /// on Kafka well under 1 s after its transaction commits (FR-006). When idle a cycle is one
    /// indexed query.
    /// </summary>
    public int PollIntervalMs { get; set; } = 250;

    /// <summary>Maximum number of entries published per cycle (a burst drains in one cycle). Default 100.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Delay after the first failed publish attempt, in milliseconds; doubles per attempt. Default 5 000.</summary>
    public int BaseDelayMs { get; set; } = 5_000;

    /// <summary>Upper bound for the backoff delay, in milliseconds. Default 600 000 (10 minutes).</summary>
    public int MaxDelayMs { get; set; } = 600_000;

    /// <summary>Failed attempts after which an entry is marked <c>Exhausted</c>. Default 8.</summary>
    public int MaxRetries { get; set; } = 8;

    /// <summary>
    /// Returns <see langword="true"/> when all values are positive and
    /// <see cref="MaxDelayMs"/> is at least <see cref="BaseDelayMs"/>.
    /// </summary>
    public bool IsValid() =>
        PollIntervalMs > 0 && BatchSize > 0 && BaseDelayMs > 0 && MaxRetries > 0 && MaxDelayMs >= BaseDelayMs;
}
