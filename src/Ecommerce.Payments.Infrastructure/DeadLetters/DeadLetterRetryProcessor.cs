using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.DeadLetters;

/// <summary>
/// One retry cycle over <c>payment_dead_letters</c> (US-03, ADR 0003). For up to
/// <see cref="DeadLetterRetryOptions.BatchSize"/> rows: open a transaction, claim one due row
/// with <c>FOR UPDATE SKIP LOCKED</c>, republish its stored payload unchanged through
/// <see cref="IRawEventPublisher"/> (which never writes a new dead letter), update the row's
/// bookkeeping and commit. One short transaction per row keeps locks bounded to a single publish
/// and lets several instances work in parallel without ever processing the same row.
/// </summary>
public sealed class DeadLetterRetryProcessor : IDeadLetterRetryProcessor
{
    private readonly IPaymentDeadLetterRepository _repository;
    private readonly IRawEventPublisher _publisher;
    private readonly DeadLetterRetryOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<DeadLetterRetryProcessor> _logger;

    /// <summary>Creates a processor for one DI scope.</summary>
    public DeadLetterRetryProcessor(
        IPaymentDeadLetterRepository repository,
        IRawEventPublisher publisher,
        IOptions<DeadLetterRetryOptions> options,
        TimeProvider timeProvider,
        ILogger<DeadLetterRetryProcessor> logger)
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

            var row = claimed[0];
            attempted++;

            try
            {
                await _publisher.PublishRawAsync(row.Topic, row.MessageKey, row.Payload, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(
                    ex,
                    "Retry of dead letter {DeadLetterId} ({EventType} for payment {PaymentId}) to {Topic} failed; " +
                    "it stays PendingRetry.",
                    row.Id,
                    row.EventType,
                    row.PaymentId,
                    row.Topic);

                await transaction.RollbackAsync(cancellationToken);
                // Without failure bookkeeping the row would still be due; stop this cycle.
                break;
            }

            DeadLetterRetryPolicy.ApplySuccess(row, _timeProvider.GetUtcNow());
            await _repository.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            _logger.LogInformation(
                "Republished dead letter {DeadLetterId} ({EventType} for payment {PaymentId}) to {Topic} on attempt {Attempt}; resolved.",
                row.Id,
                row.EventType,
                row.PaymentId,
                row.Topic,
                row.RetryCount + 1);
        }

        return attempted;
    }
}
