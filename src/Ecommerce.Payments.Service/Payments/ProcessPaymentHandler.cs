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
/// Repository (<see cref="IPaymentRepository.SaveAsync"/>) → outbox
/// (<see cref="IPaymentOutbox.AddProcessedAsync"/> or <see cref="IPaymentOutbox.AddFailedAsync"/>)
/// → commit, all in one transaction (Constitution v1.3.0, Principle II). It never publishes: the
/// outbox relay does, after the commit. Contains no business rules of its own — those belong to Domain;
/// this class never inspects <see cref="Payment.Amount"/> itself, only the *type* of the domain
/// event Domain already decided on.
/// </summary>
public sealed class ProcessPaymentHandler
{
    private readonly IPaymentRepository _repository;
    private readonly ICancellationMarkerRepository _markers;
    private readonly IOrderUnitOfWork _unitOfWork;
    private readonly IPaymentOutbox _outbox;
    private readonly decimal _maxAmountThreshold;
    private readonly ILogger<ProcessPaymentHandler> _logger;

    /// <summary>Creates the handler for one DI scope.</summary>
    public ProcessPaymentHandler(
        IPaymentRepository repository,
        ICancellationMarkerRepository markers,
        IOrderUnitOfWork unitOfWork,
        IPaymentOutbox outbox,
        IOptions<PaymentPolicyOptions> paymentPolicyOptions,
        ILogger<ProcessPaymentHandler> logger)
    {
        _repository = repository;
        _markers = markers;
        _unitOfWork = unitOfWork;
        _outbox = outbox;
        _maxAmountThreshold = paymentPolicyOptions.Value.MaxAmountThreshold;
        _logger = logger;
    }

    /// <summary>
    /// Processes a single validated <c>OrderPlaced</c> event. Inside one per-order transaction
    /// (which serialises it against an <c>OrderCancelled</c> for the same order — ADR 0004):
    /// if the order already has a payment, returns with one Information log — "duplicate delivery"
    /// when that payment was created by this same <c>eventId</c> (dedupe by <c>eventId</c> before
    /// any Domain call, Constitution II), "already exists" otherwise; returns with
    /// one Information log, creating and publishing nothing, if the order has a cancellation
    /// marker (Constitution VI "Cancellation markers"); otherwise creates and evaluates the
    /// <see cref="Payment"/>, saves it together with its outbox entry and commits; the relay
    /// publishes the entry afterwards. The whole transaction is retried for transient
    /// failures; if it never succeeds, the failure is logged and propagated so the Consumer does
    /// not commit the offset.
    /// </summary>
    public async Task HandleAsync(OrderPlacedEvent orderPlaced, CancellationToken cancellationToken)
    {
        var orderId = orderPlaced.AggregateId;
        Outcome outcome;

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

        switch (outcome)
        {
            case Outcome.DuplicateDelivery duplicate:
                _logger.LogInformation(
                    "Ignoring OrderPlaced {EventId} for order {OrderId}: duplicate delivery; payment {PaymentId} " +
                    "was created by it.",
                    orderPlaced.EventId,
                    orderId,
                    duplicate.PaymentId);
                break;
            case Outcome.AlreadyExists existing:
                _logger.LogInformation(
                    "Ignoring OrderPlaced {EventId} for order {OrderId}: payment {PaymentId} already exists " +
                    "(created by OrderPlaced {SourceEventId}).",
                    orderPlaced.EventId,
                    orderId,
                    existing.PaymentId,
                    existing.SourceEventId);
                break;
            case Outcome.OrderCancelled cancelled:
                _logger.LogInformation(
                    "Not charging order {OrderId} (OrderPlaced {EventId}): the order was already cancelled by " +
                    "OrderCancelled {CancelEventId}; no payment created and nothing published.",
                    orderId,
                    orderPlaced.EventId,
                    cancelled.CancelEventId);
                break;
        }
    }

    /// <summary>One attempt inside the per-order transaction; commits only when it writes.</summary>
    private async Task<Outcome> CreatePaymentAsync(OrderPlacedEvent orderPlaced, CancellationToken cancellationToken)
    {
        var orderId = orderPlaced.AggregateId;
        await using var transaction = await _unitOfWork.BeginAsync(orderId, cancellationToken);

        // Idempotency (Constitution II), before any Domain call: a redelivery is recognised by its
        // eventId (the one that created the payment); any other OrderPlaced for an order that
        // already has a payment is ignored too (at most one payment per order).
        var existing = await _repository.GetByOrderIdAsync(orderId, cancellationToken);
        if (existing is not null)
        {
            return existing.SourceEventId == orderPlaced.EventId
                ? new Outcome.DuplicateDelivery(existing.Id)
                : new Outcome.AlreadyExists(existing.Id, existing.SourceEventId);
        }

        // The cancellation overtook this OrderPlaced: never charge it (Constitution VI
        // "Cancellation markers").
        var marker = await _markers.GetByOrderIdAsync(orderId, cancellationToken);
        if (marker is not null)
        {
            return new Outcome.OrderCancelled(marker.SourceEventId);
        }

        var payment = Payment.CreatePending(
            orderId,
            orderPlaced.CustomerId,
            orderPlaced.TotalAmount,
            OrderPlacedEvent.DefaultCurrency,
            orderPlaced.EventId);

        var outcome = payment.Evaluate(_maxAmountThreshold);

        await _repository.SaveAsync(payment, cancellationToken);

        // The event is recorded in the same transaction as the payment (outbox). A business
        // failure is a normal outcome: it takes this same path with a different event type.
        switch (outcome)
        {
            case PaymentProcessed processed:
                await _outbox.AddProcessedAsync(processed, cancellationToken);
                break;
            case PaymentFailed failed:
                await _outbox.AddFailedAsync(failed, cancellationToken);
                break;
        }

        await transaction.CommitAsync(cancellationToken);
        return new Outcome.Created();
    }

    /// <summary>Result of one attempt; decided inside the transaction, acted on after it.</summary>
    private abstract record Outcome
    {
        public sealed record Created : Outcome;

        public sealed record DuplicateDelivery(Guid PaymentId) : Outcome;

        public sealed record AlreadyExists(Guid PaymentId, Guid SourceEventId) : Outcome;

        public sealed record OrderCancelled(Guid CancelEventId) : Outcome;
    }
}
