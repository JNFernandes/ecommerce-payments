using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Orchestrates the end-to-end workflow for one consumed <c>OrderPlaced</c> message:
/// Domain (<see cref="Payment.CreatePending"/> / <see cref="Payment.Evaluate"/>) →
/// Repository (<see cref="IPaymentRepository.SaveAsync"/>) →
/// Publisher (<see cref="IPaymentEventPublisher.PublishAsync"/> or
/// <see cref="IPaymentEventPublisher.PublishFailedAsync"/>), strictly in that order
/// (Constitution Principle II). Contains no business rules of its own — those belong to Domain;
/// this class never inspects <see cref="Payment.Amount"/> itself, only the *type* of the domain
/// event Domain already decided on.
/// </summary>
public sealed class ProcessPaymentHandler
{
    private static readonly ResiliencePipeline SavePipeline = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            MaxRetryAttempts = 3,
            Delay = TimeSpan.FromMilliseconds(200),
            BackoffType = DelayBackoffType.Exponential
        })
        .Build();

    private readonly IPaymentRepository _repository;
    private readonly IPaymentEventPublisher _publisher;
    private readonly decimal _maxAmountThreshold;
    private readonly ILogger<ProcessPaymentHandler> _logger;

    public ProcessPaymentHandler(
        IPaymentRepository repository,
        IPaymentEventPublisher publisher,
        IOptions<PaymentPolicyOptions> paymentPolicyOptions,
        ILogger<ProcessPaymentHandler> logger)
    {
        _repository = repository;
        _publisher = publisher;
        _maxAmountThreshold = paymentPolicyOptions.Value.MaxAmountThreshold;
        _logger = logger;
    }

    /// <summary>
    /// Processes a single validated <c>OrderPlaced</c> event: creates and evaluates the
    /// <see cref="Payment"/> (Domain decides Processed vs. Failed — see
    /// <see cref="Payment.Evaluate"/>), durably saves it (with a bounded retry for transient
    /// failures), then publishes the resulting event to the matching topic. The save MUST
    /// complete successfully before the publish is attempted; if it never does, the failure is
    /// logged with full context and propagated so the Consumer does not commit the offset
    /// (Kafka redelivery drives the next attempt — Constitution Principle II / User Story 3 of
    /// the companion "process payment" feature). Redelivery of an <c>OrderPlaced</c> message
    /// whose order already has any recorded outcome is a no-op — no Domain call, no save, no
    /// publish (Constitution Principle II / User Story 2).
    /// </summary>
    public async Task HandleAsync(OrderPlacedEvent orderPlaced, CancellationToken cancellationToken)
    {
        Payment payment;
        PaymentDomainEvent outcome;

        var orderId = orderPlaced.AggregateId;

        try
        {
            // Both DB calls (the idempotency check and the save) are equally subject to
            // transient PostgreSQL failures, so both are retried — not just the save.
            var alreadyExists = await SavePipeline.ExecuteAsync(
                async ct => await _repository.ExistsByOrderIdAsync(orderId, ct),
                cancellationToken);
            if (alreadyExists)
            {
                return;
            }

            payment = Payment.CreatePending(
                orderId,
                orderPlaced.CustomerId,
                orderPlaced.TotalAmount,
                OrderPlacedEvent.DefaultCurrency,
                orderPlaced.EventId);

            outcome = payment.Evaluate(_maxAmountThreshold);

            await SavePipeline.ExecuteAsync(
                async ct => await _repository.SaveAsync(payment, ct),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to durably save a Payment for order {OrderId} after retries; " +
                "no completion event will be published and the message will be retried.",
                orderId);
            throw;
        }

        // A business failure is a normal, expected outcome (Constitution FR-008 of the
        // "handle payment failure" feature) — it flows through this same success path with a
        // different event type, never through the catch block above, so it is never logged as
        // an error.
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
}
