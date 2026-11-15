using Microsoft.EntityFrameworkCore;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// PostgreSQL implementation of <see cref="IConsumedDeadLetterStore"/>: one parameterised
/// <c>INSERT … ON CONFLICT ("Topic","Partition","Offset") DO NOTHING</c>, so a crash between
/// storing and committing only causes a harmless second insert. Writes nothing to the EF change
/// tracker. <c>FailedAt</c> comes from the injected <see cref="TimeProvider"/>.
/// </summary>
public sealed class ConsumedDeadLetterStore : IConsumedDeadLetterStore
{
    private readonly PaymentsDbContext _dbContext;
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates the store over a scoped <see cref="PaymentsDbContext"/>.</summary>
    public ConsumedDeadLetterStore(PaymentsDbContext dbContext, TimeProvider timeProvider)
    {
        _dbContext = dbContext;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public async Task StoreAsync(ConsumedDeadLetter deadLetter, CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var failedAt = _timeProvider.GetUtcNow();

        await _dbContext.Database.ExecuteSqlAsync(
            $"""
            INSERT INTO consumed_dead_letters
                ("Id", "Topic", "Partition", "Offset", "MessageKey", "Payload", "Error", "Attempts", "FailedAt")
            VALUES
                ({id}, {deadLetter.Topic}, {deadLetter.Partition}, {deadLetter.Offset}, {deadLetter.MessageKey},
                 {deadLetter.Payload}, {deadLetter.Error}, {deadLetter.Attempts}, {failedAt})
            ON CONFLICT ("Topic", "Partition", "Offset") DO NOTHING
            """,
            cancellationToken);
    }
}
