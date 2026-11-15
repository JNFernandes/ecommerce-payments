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
/// "Refund rules").
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
            return new Outcome.NoPayment();
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

    private void Log(OrderCancelledEvent orderCancelled, Outcome outcome)
    {
        switch (outcome)
        {
            case Outcome.NoPayment:
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: no payment exists for the order (reason: {Reason}).",
                    orderCancelled.EventId,
                    orderCancelled.AggregateId,
                    orderCancelled.Reason);
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

        public sealed record NoPayment : Outcome;

        public sealed record DuplicateRefund(Payment Payment) : Outcome;

        public sealed record Ignored(Payment Payment, RefundIgnoredReason Reason) : Outcome;
    }
}
