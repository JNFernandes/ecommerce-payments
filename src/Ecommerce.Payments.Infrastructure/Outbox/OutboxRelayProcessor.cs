using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>
/// One outbox relay cycle over <c>payment_outbox</c> (US-07, ADR 0006; claim and backoff design
/// from ADR 0003). For up to <see cref="OutboxRelayOptions.BatchSize"/> entries: open a
/// transaction, claim the next due entry — recording order, never ahead of an unpublished earlier
/// entry for the same payment, <c>FOR UPDATE SKIP LOCKED</c> — publish its stored payload
/// unchanged through <see cref="IRawEventPublisher"/>, update its bookkeeping
/// (<see cref="OutboxRetryPolicy"/>: Published, rescheduled with backoff, or Exhausted) and
/// commit. One short transaction per entry keeps locks bounded to a single publish and lets
/// several instances work in parallel without ever publishing the same entry concurrently. If
/// the cycle is cancelled mid-publish, the entry's transaction rolls back and it stays due.
/// </summary>
public sealed class OutboxRelayProcessor : IOutboxRelayProcessor
{
    private readonly IPaymentOutboxRepository _repository;
    private readonly IRawEventPublisher _publisher;
    private readonly OutboxRelayOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<OutboxRelayProcessor> _logger;

    /// <summary>Creates a processor for one DI scope.</summary>
    public OutboxRelayProcessor(
        IPaymentOutboxRepository repository,
        IRawEventPublisher publisher,
        IOptions<OutboxRelayOptions> options,
        TimeProvider timeProvider,
        ILogger<OutboxRelayProcessor> logger)
    {
        _repository = repository;
        _publisher = publisher;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<int> RunCycleAsync(CancellationToken cancellationToken)
    {
        var attempted = 0;

        while (attempted < _options.BatchSize)
        {
            await using var transaction = await _repository.BeginTransactionAsync(cancellationToken);

            var claimed = await _repository.ClaimDueAsync(_timeProvider.GetUtcNow(), 1, cancellationToken);
            if (claimed.Count == 0)
            {
                await transaction.CommitAsync(cancellationToken);
                break;
            }

            var entry = claimed[0];
            attempted++;

            try
            {
                await _publisher.PublishRawAsync(entry.Topic, entry.MessageKey, entry.Payload, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The failure is recorded on the same entry; nothing new is written.
                var exhausted = OutboxRetryPolicy.ApplyFailure(entry, ex.Message, _timeProvider.GetUtcNow(), _options);
                await _repository.SaveChangesAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                await LogFailureAsync(entry, ex, exhausted, cancellationToken);
                continue;
            }

            OutboxRetryPolicy.ApplySuccess(entry, _timeProvider.GetUtcNow());
            await _repository.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            if (entry.RetryCount > 0)
            {
                _logger.LogInformation(
                    "Published outbox entry {EntryId} ({EventType} for payment {PaymentId}) to {Topic} after {RetryCount} failed attempt(s).",
                    entry.Id,
                    entry.EventType,
                    entry.PaymentId,
                    entry.Topic,
                    entry.RetryCount);
            }
            else
            {
                _logger.LogDebug(
                    "Published outbox entry {EntryId} ({EventType} for payment {PaymentId}) to {Topic}.",
                    entry.Id,
                    entry.EventType,
                    entry.PaymentId,
                    entry.Topic);
            }
        }

        return attempted;
    }

    private async Task LogFailureAsync(OutboxEntryEntity entry, Exception error, bool exhausted, CancellationToken cancellationToken)
    {
        if (exhausted)
        {
            // Later events for this payment wait behind it (per-payment ordering) until re-queued.
            var blocked = await _repository.CountBlockedAfterAsync(entry.PaymentId, entry.Sequence, cancellationToken);
            _logger.LogError(
                error,
                "Outbox entry {EntryId} ({EventType} for payment {PaymentId}) to {Topic} is Exhausted after " +
                "{RetryCount} retries and will not be retried again; {Blocked} later event(s) for the same payment " +
                "are waiting behind it. Last error: {LastError}",
                entry.Id,
                entry.EventType,
                entry.PaymentId,
                entry.Topic,
                entry.RetryCount,
                blocked,
                entry.LastError);
            return;
        }

        _logger.LogWarning(
            error,
            "Publish attempt {RetryCount} of outbox entry {EntryId} ({EventType} for payment {PaymentId}) to {Topic} " +
            "failed; next attempt at {NextAttemptAt}. Error: {LastError}",
            entry.RetryCount,
            entry.Id,
            entry.EventType,
            entry.PaymentId,
            entry.Topic,
            entry.NextAttemptAt,
            entry.LastError);
    }
}
