using System.Collections.Concurrent;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.Payments;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// In-memory <see cref="IPaymentEventPublisher"/> test double. Integration tests exercise
/// Consumer → Repository against a real PostgreSQL container; Kafka is out of scope at this
/// tier (see plan.md Testing), so publishing is recorded rather than sent over the wire.
/// </summary>
public sealed class FakePaymentEventPublisher : IPaymentEventPublisher
{
    public ConcurrentBag<PaymentProcessed> Published { get; } = [];

    public Task PublishAsync(PaymentProcessed paymentProcessed, CancellationToken cancellationToken)
    {
        Published.Add(paymentProcessed);
        return Task.CompletedTask;
    }
}
