using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Orders;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Orchestrates the end-to-end workflow for one consumed <c>OrderPlaced</c> message:
/// per-order transaction (<see cref="IOrderUnitOfWork"/>) → idempotency check →
/// Domain (<see cref="Payment.CreatePending"/> / <see cref="Payment.Evaluate"/>) →
/// Repository (<see cref="IPaymentRepository.SaveAsync"/>) → commit →
/// Publisher (<see cref="IPaymentEventPublisher.PublishAsync"/> or
/// <see cref="IPaymentEventPublisher.PublishFailedAsync"/>), strictly in that order
/// (Constitution Principle II). Contains no business rules of its own — those belong to Domain;
/// this class never inspects <see cref="Payment.Amount"/> itself, only the *type* of the domain
/// event Domain already decided on.
/// </summary>
public sealed class ProcessPaymentHandler
{
    private readonly IPaymentRepository _repository;
    private readonly ICancellationMarkerRepository _markers;
    private readonly IOrderUnitOfWork _unitOfWork;
    private readonly IPaymentEventPublisher _publisher;
    private readonly decimal _maxAmountThreshold;
    private readonly ILogger<ProcessPaymentHandler> _logger;

    /// <summary>Creates the handler for one DI scope.</summary>
    public ProcessPaymentHandler(
        IPaymentRepository repository,
        ICancellationMarkerRepository markers,
        IOrderUnitOfWork unitOfWork,
        IPaymentEventPublisher publisher,
        IOptions<PaymentPolicyOptions> paymentPolicyOptions,
        ILogger<ProcessPaymentHandler> logger)
    {
        _repository = repository;
        _markers = markers;
        _unitOfWork = unitOfWork;
        _publisher = publisher;
        _maxAmountThreshold = paymentPolicyOptions.Value.MaxAmountThreshold;
        _logger = logger;
    }

    /// <summary>
    /// Processes a single validated <c>OrderPlaced</c> event. Inside one per-order transaction
    /// (which serialises it against an <c>OrderCancelled</c> for the same order — ADR 0004):
    /// returns if the order already has a payment (idempotency, Constitution II), otherwise
    /// creates and evaluates the <see cref="Payment"/>, saves it and commits. Only after the
    /// commit is the resulting event published. The whole transaction is retried for transient
    /// failures; if it never succeeds, the failure is logged and propagated so the Consumer does
    /// not commit the offset.
    /// </summary>
    public async Task HandleAsync(OrderPlacedEvent orderPlaced, CancellationToken cancellationToken)
    {
        var orderId = orderPlaced.AggregateId;
        PaymentDomainEvent? outcome;

        try
        {
            // The whole unit is retried: after a failure the transaction is aborted, so the
            // reads must be repeated under a fresh lock.
            outcome = await PersistenceRetry.Pipeline.ExecuteAsync(
                async ct => await CreatePaymentAsync(orderPlaced, ct),
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "Failed to durably save a Payment for order {OrderId} after retries; " +
                "no completion event will be published and the message will be retried.",
                orderId);
            throw;
        }

        // A business failure is a normal, expected outcome — it flows through this same success
        // path with a different event type, never through the catch block above.
        switch (outcome)
        {
            case PaymentProcessed processed:
                await _publisher.PublishAsync(processed, cancellationToken);
                break;
            case PaymentFailed failed:
                await _publisher.PublishFailedAsync(failed, cancellationToken);
                break;
        }
    }

    /// <summary>One attempt; returns the event to publish, or null when there is nothing to do.</summary>
    private async Task<PaymentDomainEvent?> CreatePaymentAsync(OrderPlacedEvent orderPlaced, CancellationToken cancellationToken)
    {
        var orderId = orderPlaced.AggregateId;
        await using var transaction = await _unitOfWork.BeginAsync(orderId, cancellationToken);

        if (await _repository.ExistsByOrderIdAsync(orderId, cancellationToken))
        {
            return null;
        }

        var payment = Payment.CreatePending(
            orderId,
            orderPlaced.CustomerId,
            orderPlaced.TotalAmount,
            OrderPlacedEvent.DefaultCurrency,
            orderPlaced.EventId);

        var outcome = payment.Evaluate(_maxAmountThreshold);

        await _repository.SaveAsync(payment, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return outcome;
    }
}
