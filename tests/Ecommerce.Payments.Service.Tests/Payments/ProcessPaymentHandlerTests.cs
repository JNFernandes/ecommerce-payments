using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

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

    private static ProcessPaymentHandler CreateHandler(
        IPaymentRepository repository,
        IPaymentEventPublisher publisher,
        decimal maxAmountThreshold = decimal.MaxValue) =>
        new(
            repository,
            publisher,
            Options.Create(new PaymentPolicyOptions { MaxAmountThreshold = maxAmountThreshold }),
            NullLogger<ProcessPaymentHandler>.Instance);

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

        var handler = CreateHandler(repository.Object, publisher.Object);

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

        var handler = CreateHandler(repository.Object, publisher.Object);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.HandleAsync(ValidOrderPlaced(), CancellationToken.None));

        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
        publisher.Verify(p => p.PublishFailedAsync(It.IsAny<PaymentFailed>(), It.IsAny<CancellationToken>()), Times.Never);
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

        var handler = CreateHandler(repository.Object, publisher.Object);

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

        var handler = CreateHandler(repository.Object, publisher.Object);

        await handler.HandleAsync(ValidOrderPlaced(), CancellationToken.None);

        repository.Verify(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Never);
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_AmountAboveThreshold_PublishesFailedNotProcessedAndDoesNotLogAsError()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var orderPlaced = ValidOrderPlaced();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        repository
            .Setup(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        publisher
            .Setup(p => p.PublishFailedAsync(It.IsAny<PaymentFailed>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var handler = CreateHandler(repository.Object, publisher.Object, maxAmountThreshold: orderPlaced.TotalAmount - 1m);

        await handler.HandleAsync(orderPlaced, CancellationToken.None);

        publisher.Verify(p => p.PublishFailedAsync(It.IsAny<PaymentFailed>(), It.IsAny<CancellationToken>()), Times.Once);
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_AmountAtOrBelowThreshold_PublishesProcessedNotFailed()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var orderPlaced = ValidOrderPlaced();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        repository
            .Setup(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        publisher
            .Setup(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var handler = CreateHandler(repository.Object, publisher.Object, maxAmountThreshold: orderPlaced.TotalAmount);

        await handler.HandleAsync(orderPlaced, CancellationToken.None);

        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Once);
        publisher.Verify(p => p.PublishFailedAsync(It.IsAny<PaymentFailed>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
