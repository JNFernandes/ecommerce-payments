namespace Ecommerce.Payments.Infrastructure.Retention;

/// <summary>Rows deleted by one retention purge run, per store (US-09).</summary>
/// <param name="OutboxEntries">Published <c>payment_outbox</c> entries deleted.</param>
/// <param name="DeadLetters"><c>consumed_dead_letters</c> rows deleted.</param>
/// <param name="Markers"><c>order_cancellation_markers</c> rows deleted.</param>
public sealed record PurgeResult(int OutboxEntries, int DeadLetters, int Markers)
{
    /// <summary>Total rows deleted across all stores.</summary>
    public int Total => OutboxEntries + DeadLetters + Markers;
}
