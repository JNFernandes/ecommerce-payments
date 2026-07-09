using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Orchestrates the end-to-end workflow for one consumed <c>OrderPlaced</c> message:
/// Domain (<see cref="Payment.CreatePending"/> / <see cref="Payment.Process"/>) →
/// Repository (<see cref="IPaymentRepository.SaveAsync"/>) →
/// Publisher (<see cref="IPaymentEventPublisher.PublishAsync"/>), strictly in that order
/// (Constitution Principle II). Contains no business rules of its own — those belong to Domain.
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
    private readonly ILogger<ProcessPaymentHandler> _logger;

    public ProcessPaymentHandler(
        IPaymentRepository repository,
        IPaymentEventPublisher publisher,
        ILogger<ProcessPaymentHandler> logger)
    {
        _repository = repository;
        _publisher = publisher;
        _logger = logger;
    }

    /// <summary>
    /// Processes a single validated <c>OrderPlaced</c> event: creates and processes the
    /// <see cref="Payment"/>, durably saves it (with a bounded retry for transient failures),
    /// then publishes the completion event. The save MUST complete successfully before the
    /// publish is attempted; if it never does, the failure is logged with full context and
    /// propagated so the Consumer does not commit the offset (Kafka redelivery drives the next
    /// attempt — Constitution Principle II / User Story 3). Redelivery of an <c>OrderPlaced</c>
    /// message whose order already has a <see cref="Payment"/> is a no-op — no Domain call, no
    /// save, no publish (Constitution Principle II / User Story 2).
    /// </summary>
    public async Task HandleAsync(OrderPlacedEvent orderPlaced, CancellationToken cancellationToken)
    {
        Payment payment;
        PaymentProcessed paymentProcessed;

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

            paymentProcessed = payment.Process();

            await SavePipeline.ExecuteAsync(
                async ct => await _repository.SaveAsync(payment, ct),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to durably save a Payment for order {OrderId} after retries; " +
                "no PaymentProcessed will be published and the message will be retried.",
                orderId);
            throw;
        }

        await _publisher.PublishAsync(paymentProcessed, cancellationToken);
    }
}
