using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Payments;
using Moq;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ecommerce.Payments.Service.Tests.Payments;

public class ProcessPaymentHandlerTests
{
    private static OrderPlacedEvent ValidOrderPlaced() => new()
    {
        EventId = Guid.NewGuid(),
        OccurredAt = DateTimeOffset.UtcNow,
        AggregateId = Guid.NewGuid(),
        Version = 1,
        CustomerId = Guid.NewGuid(),
        TotalAmount = 129.99m
    };

    [Fact]
    public async Task HandleAsync_ValidOrderPlaced_CallsRepositoryThenPublisherInOrder()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var callOrder = new List<string>();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        repository
            .Setup(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("Repository"))
            .Returns(Task.CompletedTask);
        publisher
            .Setup(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("Publisher"))
            .Returns(Task.CompletedTask);

        var handler = new ProcessPaymentHandler(repository.Object, publisher.Object, NullLogger<ProcessPaymentHandler>.Instance);

        await handler.HandleAsync(ValidOrderPlaced(), CancellationToken.None);

        Assert.Equal(["Repository", "Publisher"], callOrder);
        repository.Verify(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Once);
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandleAsync_RepositoryThrows_NeverCallsPublisher()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        repository
            .Setup(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("transient DB failure"));

        var handler = new ProcessPaymentHandler(repository.Object, publisher.Object, NullLogger<ProcessPaymentHandler>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.HandleAsync(ValidOrderPlaced(), CancellationToken.None));

        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_RepositoryThrowsPersistently_RetriesThenThrowsWithoutCallingPublisher()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var attempts = 0;

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        repository
            .Setup(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .Callback(() => attempts++)
            .ThrowsAsync(new InvalidOperationException("persistent DB outage"));

        var handler = new ProcessPaymentHandler(repository.Object, publisher.Object, NullLogger<ProcessPaymentHandler>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.HandleAsync(ValidOrderPlaced(), CancellationToken.None));

        Assert.True(attempts > 1, "Expected the handler to retry the save before giving up.");
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_PaymentAlreadyExistsForOrder_SkipsAsNoOp()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var handler = new ProcessPaymentHandler(repository.Object, publisher.Object, NullLogger<ProcessPaymentHandler>.Instance);

        await handler.HandleAsync(ValidOrderPlaced(), CancellationToken.None);

        repository.Verify(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Never);
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
