using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Microsoft.Extensions.Logging;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Orchestrates one consumed <c>OrderCancelled</c> message (US-04):
/// Repository (<see cref="IPaymentRepository.GetByOrderIdAsync"/>) →
/// <c>eventId</c> dedupe → Domain (<see cref="Payment.RequestRefund"/>) →
/// Repository (<see cref="IPaymentRepository.SaveRefundAsync"/>) →
/// Publisher (<see cref="IPaymentEventPublisher.PublishRefundedAsync"/>), strictly in that order
/// (Constitution Principle II). The refund rule lives in Domain; this class only switches on
/// the <see cref="RefundDecision"/> type. Cancellations that need no refund are normal outcomes:
/// they return without error so the Consumer commits the offset (Constitution VI
/// "Refund rules").
/// </summary>
public sealed class RefundPaymentHandler
{
    private readonly IPaymentRepository _repository;
    private readonly IPaymentEventPublisher _publisher;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RefundPaymentHandler> _logger;

    /// <summary>Creates the handler for one DI scope.</summary>
    public RefundPaymentHandler(
        IPaymentRepository repository,
        IPaymentEventPublisher publisher,
        TimeProvider timeProvider,
        ILogger<RefundPaymentHandler> logger)
    {
        _repository = repository;
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
        var orderId = orderCancelled.AggregateId;
        PaymentRefunded refunded;

        try
        {
            var payment = await PersistenceRetry.Pipeline.ExecuteAsync(
                async ct => await _repository.GetByOrderIdAsync(orderId, ct),
                cancellationToken);

            if (payment is null)
            {
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: no payment exists for the order (reason: {Reason}).",
                    orderCancelled.EventId,
                    orderId,
                    orderCancelled.Reason);
                return;
            }

            // eventId dedupe before any Domain call (Constitution II): this exact message already
            // caused the refund.
            if (payment.RefundSourceEventId == orderCancelled.EventId)
            {
                _logger.LogInformation(
                    "Ignoring OrderCancelled {EventId} for order {OrderId}: duplicate delivery; payment {PaymentId} " +
                    "was already refunded by it.",
                    orderCancelled.EventId,
                    orderId,
                    payment.Id);
                return;
            }

            var decision = payment.RequestRefund(orderCancelled.EventId, _timeProvider.GetUtcNow());
            if (decision is RefundDecision.Ignored ignored)
            {
                LogIgnored(orderCancelled, payment, ignored.Reason);
                return;
            }

            var refundedDecision = (RefundDecision.Refunded)decision;
            var saved = await PersistenceRetry.Pipeline.ExecuteAsync(
                async ct => await _repository.SaveRefundAsync(payment, ct),
                cancellationToken);
            if (!saved)
            {
                // Another delivery refunded it between our read and our conditional update.
                LogIgnored(orderCancelled, payment, RefundIgnoredReason.AlreadyRefunded);
                return;
            }

            refunded = refundedDecision.Event;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "Failed to load or save the refund for order {OrderId} (cancellation {EventId}) after retries; " +
                "nothing published, the message will be retried.",
                orderId,
                orderCancelled.EventId);
            throw;
        }

        await _publisher.PublishRefundedAsync(refunded, cancellationToken);
    }

    private void LogIgnored(OrderCancelledEvent orderCancelled, Payment payment, RefundIgnoredReason reason)
    {
        if (reason == RefundIgnoredReason.AlreadyRefunded)
        {
            _logger.LogInformation(
                "Ignoring OrderCancelled {EventId} for order {OrderId}: payment {PaymentId} was already refunded.",
                orderCancelled.EventId,
                orderCancelled.AggregateId,
                payment.Id);
            return;
        }

        _logger.LogInformation(
            "Ignoring OrderCancelled {EventId} for order {OrderId}: payment {PaymentId} is {Status} and was never " +
            "completed; nothing to refund (reason: {Reason}).",
            orderCancelled.EventId,
            orderCancelled.AggregateId,
            payment.Id,
            payment.Status,
            orderCancelled.Reason);
    }
}
