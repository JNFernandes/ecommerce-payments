using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Retention;

/// <summary>
/// PostgreSQL implementation of <see cref="IRetentionPurger"/> (US-09, ADR 0007; Constitution
/// v1.4.0, Principle VI "Data retention"). For each store whose retention is above zero it
/// deletes rows older than <c>now − days</c>, one bounded batch per auto-committed statement:
/// <c>DELETE … WHERE key IN (SELECT key … ORDER BY time LIMIT n FOR UPDATE SKIP LOCKED)</c>,
/// repeated until a batch deletes fewer than <see cref="RetentionOptions.BatchSize"/> rows.
/// <c>SKIP LOCKED</c> lets several instances purge at once without waiting on each other or
/// deleting a row twice, and leaves rows locked by the outbox relay alone. Only
/// <c>Published</c> outbox entries are ever eligible — <c>Pending</c> and <c>Exhausted</c> ones,
/// and payments, are never touched. Deleting a published entry cannot block or reorder later
/// entries for the same payment: the relay only waits on earlier entries that exist and are not
/// published.
/// </summary>
public sealed class RetentionPurger : IRetentionPurger
{
    private readonly PaymentsDbContext _dbContext;
    private readonly RetentionOptions _options;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the purger for one DI scope.</summary>
    public RetentionPurger(PaymentsDbContext dbContext, IOptions<RetentionOptions> options, TimeProvider timeProvider)
    {
        _dbContext = dbContext;
        _options = options.Value;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public async Task<PurgeResult> PurgeAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var batch = _options.BatchSize;
        var published = nameof(OutboxStatus.Published);

        var outbox = 0;
        if (_options.OutboxPublishedDays > 0)
        {
            var cutoff = now.AddDays(-_options.OutboxPublishedDays);
            outbox = await PurgeStoreAsync(
                $"""
                DELETE FROM payment_outbox WHERE "Id" IN (
                    SELECT "Id" FROM payment_outbox
                    WHERE "Status" = {published} AND "PublishedAt" < {cutoff}
                    ORDER BY "PublishedAt"
                    LIMIT {batch}
                    FOR UPDATE SKIP LOCKED)
                """,
                cancellationToken);
        }

        var deadLetters = 0;
        if (_options.ConsumedDeadLettersDays > 0)
        {
            var cutoff = now.AddDays(-_options.ConsumedDeadLettersDays);
            deadLetters = await PurgeStoreAsync(
                $"""
                DELETE FROM consumed_dead_letters WHERE "Id" IN (
                    SELECT "Id" FROM consumed_dead_letters
                    WHERE "FailedAt" < {cutoff}
                    ORDER BY "FailedAt"
                    LIMIT {batch}
                    FOR UPDATE SKIP LOCKED)
                """,
                cancellationToken);
        }

        var markers = 0;
        if (_options.CancellationMarkersDays > 0)
        {
            var cutoff = now.AddDays(-_options.CancellationMarkersDays);
            markers = await PurgeStoreAsync(
                $"""
                DELETE FROM order_cancellation_markers WHERE "OrderId" IN (
                    SELECT "OrderId" FROM order_cancellation_markers
                    WHERE "RecordedAt" < {cutoff}
                    ORDER BY "RecordedAt"
                    LIMIT {batch}
                    FOR UPDATE SKIP LOCKED)
                """,
                cancellationToken);
        }

        return new PurgeResult(outbox, deadLetters, markers);
    }

    /// <summary>Runs one batch statement repeatedly until it deletes fewer rows than a full batch.</summary>
    private async Task<int> PurgeStoreAsync(FormattableString batchStatement, CancellationToken cancellationToken)
    {
        var total = 0;
        int deleted;
        do
        {
            deleted = await _dbContext.Database.ExecuteSqlAsync(batchStatement, cancellationToken);
            total += deleted;
        }
        while (deleted >= _options.BatchSize);

        return total;
    }
}
