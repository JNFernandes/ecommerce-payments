namespace Ecommerce.Payments.Infrastructure.Retention;

/// <summary>
/// Data-retention settings (US-09, ADR 0007; Constitution v1.4.0, Principle VI "Data
/// retention"), bound from the <c>Retention</c> configuration section and validated at startup
/// with <see cref="IsValid"/>. A retention of <c>0</c> days keeps that store forever.
/// </summary>
public sealed class RetentionOptions
{
    /// <summary>Configuration section name.</summary>
    public const string SectionName = "Retention";

    /// <summary>Time between purge runs, in milliseconds. Default 3 600 000 (1 hour).</summary>
    public int IntervalMs { get; set; } = 3_600_000;

    /// <summary>Maximum rows deleted per statement; a run repeats batches until a store is drained. Default 1 000.</summary>
    public int BatchSize { get; set; } = 1_000;

    /// <summary>
    /// Days a <c>Published</c> outbox entry is kept after it was published. Default 30; 0 keeps
    /// them forever. <c>Pending</c> and <c>Exhausted</c> entries are never purged.
    /// </summary>
    public int OutboxPublishedDays { get; set; } = 30;

    /// <summary>
    /// Days an inbound dead letter is kept after it was stored. Default 90; 0 keeps them forever.
    /// Replay or export rows you still need before they expire.
    /// </summary>
    public int ConsumedDeadLettersDays { get; set; } = 90;

    /// <summary>
    /// Days a cancellation marker is kept after it was recorded. Default 180; 0 keeps them forever.
    /// Must be longer than the longest expected delay of an <c>OrderPlaced</c> behind its
    /// <c>OrderCancelled</c>: a marker purged too early lets a very late <c>OrderPlaced</c> charge a
    /// cancelled order.
    /// </summary>
    public int CancellationMarkersDays { get; set; } = 180;

    /// <summary>
    /// Returns <see langword="true"/> when the interval and batch size are positive and every
    /// retention is zero or more.
    /// </summary>
    public bool IsValid() =>
        IntervalMs > 0
        && BatchSize > 0
        && OutboxPublishedDays >= 0
        && ConsumedDeadLettersDays >= 0
        && CancellationMarkersDays >= 0;
}
