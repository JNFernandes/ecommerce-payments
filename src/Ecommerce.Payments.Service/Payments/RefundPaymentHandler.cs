using Ecommerce.Payments.Domain.Orders;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Orders;
using Microsoft.Extensions.Logging;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Orchestrates one consumed <c>OrderCancelled</c> message (US-04):
/// per-order transaction (<see cref="IOrderUnitOfWork"/>) →
/// Repository (<see cref="IPaymentRepository.GetByOrderIdAsync"/>) →
/// <c>eventId</c> dedupe → Domain (<see cref="Payment.RequestRefund"/>) →
/// Repository (<see cref="IPaymentRepository.SaveRefundAsync"/>) → commit →
/// Publisher (<see cref="IPaymentEventPublisher.PublishRefundedAsync"/>), strictly in that order
/// (Constitution Principle II). The refund rule lives in Domain; this class only switches on
/// the <see cref="RefundDecision"/> type. Cancellations that need no refund are normal outcomes:
/// they return without error so the Consumer commits the offset (Constitution VI
/// "Refund rules"). When the order has no payment yet, a <see cref="CancellationMarker"/> is
/// recorded instead (US-05, Constitution VI "Cancellation markers"); the per-order transaction
/// makes that mutually exclusive with payment creation (ADR 0004).
/// </summary>
public sealed class RefundPaymentHandler
{
    private readonly IPaymentRepository _repository;
    private readonly ICancellationMarkerRepository _markers;
    private readonly IOrderUnitOfWork _unitOfWork;
    private readonly IPaymentEventPublisher _publisher;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RefundPaymentHandler> _logger;

    /// <summary>Creates the handler for one DI scope.</summary>
    public RefundPaymentHandler(
        IPaymentRepository repository,
        ICancellationMarkerRepository markers,
        IOrderUnitOfWork unitOfWork,
        IPaymentEventPublisher publisher,
        TimeProvider timeProvider,
        ILogger<RefundPaymentHandler> logger)
    {
        _repository = repository;
        _markers = markers;
        _unitOfWork = unitOfWork;
        _publisher = publisher;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <summary>
    /// Refunds the order's payment if it is Processed, saving before publishing. Returns normally
    /// when there is nothing to refund. Throws (after logging) only when the database stays
    /// unavailable through the bounded retries, so the Consumer retries the message.
    /// </summary>
    public async Task HandleAsync(OrderCancelledEvent orderCancelled, CancellationToken cancellationToken)
    {
        Outcome outcome;

        try
        {
            outcome = await PersistenceRetry.Pipeline.ExecuteAsync(
                async ct => await DecideAsync(orderCancelled, ct),
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "Failed to load or save the refund for order {OrderId} (cancellation {EventId}) after retries; " +
                "nothing published, the message will be retried.",
                orderCancelled.AggregateId,
                orderCancelled.EventId);
            throw;
        }

        Log(orderCancelled, outcome);

        if (outcome is Outcome.Refunded refunded)
        {
            await _publisher.PublishRefundedAsync(refunded.Event, cancellationToken);
        }
    }

    /// <summary>One attempt inside the per-order transaction; commits only when it writes.</summary>
    private async Task<Outcome> DecideAsync(OrderCancelledEvent orderCancelled, CancellationToken cancellationToken)
    {
        var orderId = orderCancelled.AggregateId;
        await using var transaction = await _unitOfWork.BeginAsync(orderId, cancellationToken);

        var payment = await _repository.GetByOrderIdAsync(orderId, cancellationToken);
        if (payment is null)
        {
            return await RecordMarkerAsync(orderCancelled, transaction, cancellationToken);
        }

        // eventId dedupe before any Domain call (Constitution II): this exact message already
        // caused the refund.
        if (payment.RefundSourceEventId == orderCancelled.EventId)
        {
            return new Outcome.DuplicateRefund(payment);
        }

        var decision = payment.RequestRefund(orderCancelled.EventId, _timeProvider.GetUtcNow());
        if (decision is RefundDecision.Ignored ignored)
        {
            return new Outcome.Ignored(payment, ignored.Reason);
        }

        var refundedDecision = (RefundDecision.Refunded)decision;
        if (!await _repository.SaveRefundAsync(payment, cancellationToken))
        {
            // Another delivery refunded it between our read and our conditional update.
            return new Outcome.Ignored(payment, RefundIgnoredReason.AlreadyRefunded);
        }

        await transaction.CommitAsync(cancellationToken);
        return new Outcome.Refunded(refundedDecision.Event);
    }

    /// <summary>
    /// No payment exists yet: the cancellation may have overtaken its <c>OrderPlaced</c>. Records
    /// a <see cref="CancellationMarker"/> so that a later <c>OrderPlaced</c> is not charged
    /// (Constitution VI "Cancellation markers"), unless the order already has one.
    /// </summary>
    private async Task<Outcome> RecordMarkerAsync(
        OrderCancelledEvent orderCancelled,
        IOrderTransaction transaction,
        CancellationToken cancellationToken)
    {
        var existing = await _markers.GetByOrderIdAsync(orderCancelled.AggregateId, cancellationToken);
        if (existing is not null)
        {
            return existing.SourceEventId == orderCancelled.EventId
                ? new Outcome.DuplicateMarker()
                : new Outcome.AlreadyMarked(existing.SourceEventId);
        }

        var marker = CancellationMarker.Record(
            orderCancelled.AggregateId,
            orderCancelled.EventId,
            orderCancelled.OccurredAt,
            _timeProvider.GetUtcNow(),
            orderCancelled.Reason);
        await _markers.AddAsync(marker, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new Outcome.MarkerRecorded();
    }

    private void Log(OrderCancelledEvent orderCancelled, Outcome outcome)
    {
        switch (outcome)
        {
            case Outcome.MarkerRecorded:
                _logger.LogInformation(
                    "Recorded cancellation of order {OrderId} from OrderCancelled {EventId} before any payment exists; " +
                    "a later OrderPlaced will not be charged (reason: {Reason}).",
                    orderCancelled.AggregateId,
                    orderCancelled.EventId,
                    orderCancelled.Reason);
                break;
            case Outcome.DuplicateMarker:
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: duplicate delivery; cancellation already recorded.",
                    orderCancelled.EventId,
                    orderCancelled.AggregateId);
                break;
            case Outcome.AlreadyMarked marked:
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: order already recorded as cancelled by {SourceEventId}.",
                    orderCancelled.EventId,
                    orderCancelled.AggregateId,
                    marked.SourceEventId);
                break;
            case Outcome.DuplicateRefund duplicate:
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: duplicate delivery; payment {PaymentId} " +
                    "was already refunded by it.",
                    orderCancelled.EventId,
                    orderCancelled.AggregateId,
                    duplicate.Payment.Id);
                break;
            case Outcome.Ignored { Reason: RefundIgnoredReason.AlreadyRefunded } ignored:
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: payment {PaymentId} was already refunded.",
                    orderCancelled.EventId,
                    orderCancelled.AggregateId,
                    ignored.Payment.Id);
                break;
            case Outcome.Ignored ignored:
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: payment {PaymentId} is {Status} and was never " +
                    "completed; nothing to refund (reason: {Reason}).",
                    orderCancelled.EventId,
                    orderCancelled.AggregateId,
                    ignored.Payment.Id,
                    ignored.Payment.Status,
                    orderCancelled.Reason);
                break;
        }
    }

    /// <summary>Result of one handling attempt; decided inside the transaction, logged after it.</summary>
    private abstract record Outcome
    {
        public sealed record Refunded(PaymentRefunded Event) : Outcome;

        public sealed record MarkerRecorded : Outcome;

        public sealed record DuplicateMarker : Outcome;

        public sealed record AlreadyMarked(Guid SourceEventId) : Outcome;

        public sealed record DuplicateRefund(Payment Payment) : Outcome;

        public sealed record Ignored(Payment Payment, RefundIgnoredReason Reason) : Outcome;
    }
}
