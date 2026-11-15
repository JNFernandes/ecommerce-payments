using Ecommerce.Payments.Domain.Orders;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Orders;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Logging;
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
        decimal maxAmountThreshold = decimal.MaxValue,
        ICancellationMarkerRepository? markers = null,
        IOrderUnitOfWork? unitOfWork = null,
        ILogger<ProcessPaymentHandler>? logger = null) =>
        new(
            repository,
            markers ?? new Mock<ICancellationMarkerRepository>().Object,
            unitOfWork ?? new RecordingOrderUnitOfWork(),
            publisher,
            Options.Create(new PaymentPolicyOptions { MaxAmountThreshold = maxAmountThreshold }),
            logger ?? NullLogger<ProcessPaymentHandler>.Instance);

    [Fact]
    public async Task Should_Commit_Order_Transaction_Before_Publishing()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var calls = new List<string>();
        var unitOfWork = new RecordingOrderUnitOfWork(calls);
        var orderPlaced = ValidOrderPlaced();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        repository
            .Setup(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .Callback(() => calls.Add("Save"))
            .Returns(Task.CompletedTask);
        publisher
            .Setup(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()))
            .Callback(() => calls.Add("Publish"))
            .Returns(Task.CompletedTask);

        await CreateHandler(repository.Object, publisher.Object, unitOfWork: unitOfWork)
            .HandleAsync(orderPlaced, CancellationToken.None);

        Assert.Equal([$"Begin:{orderPlaced.AggregateId}", "Save", "Commit", "Dispose", "Publish"], calls);
    }

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

    [Fact]
    public async Task Should_Not_Create_Or_Publish_Payment_When_Order_Has_Cancellation_Marker()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var markers = new Mock<ICancellationMarkerRepository>();
        var unitOfWork = new RecordingOrderUnitOfWork();
        var logger = new ListLogger<ProcessPaymentHandler>();
        var orderPlaced = ValidOrderPlaced();
        var cancelEventId = Guid.NewGuid();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        markers
            .Setup(m => m.GetByOrderIdAsync(orderPlaced.AggregateId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CancellationMarker.Rehydrate(
                orderPlaced.AggregateId, cancelEventId, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null));

        await CreateHandler(repository.Object, publisher.Object, markers: markers.Object, unitOfWork: unitOfWork, logger: logger)
            .HandleAsync(orderPlaced, CancellationToken.None);

        repository.Verify(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Never);
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Never);
        publisher.Verify(p => p.PublishFailedAsync(It.IsAny<PaymentFailed>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Equal(0, unitOfWork.Commits);
        var (level, message) = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Information, level);
        Assert.Contains("already cancelled", message, StringComparison.Ordinal);
        Assert.Contains(orderPlaced.AggregateId.ToString(), message, StringComparison.Ordinal);
        Assert.Contains(orderPlaced.EventId.ToString(), message, StringComparison.Ordinal);
        Assert.Contains(cancelEventId.ToString(), message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Should_Check_Payment_Before_Marker()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var markers = new Mock<ICancellationMarkerRepository>();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        await CreateHandler(repository.Object, publisher.Object, markers: markers.Object)
            .HandleAsync(ValidOrderPlaced(), CancellationToken.None);

        markers.Verify(m => m.GetByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
        repository.Verify(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Should_Process_Normally_When_No_Marker()
    {
        var repository = new Mock<IPaymentRepository>();
        var publisher = new Mock<IPaymentEventPublisher>();
        var markers = new Mock<ICancellationMarkerRepository>();
        var orderPlaced = ValidOrderPlaced();

        repository
            .Setup(r => r.ExistsByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        markers
            .Setup(m => m.GetByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((CancellationMarker?)null);

        await CreateHandler(repository.Object, publisher.Object, markers: markers.Object)
            .HandleAsync(orderPlaced, CancellationToken.None);

        markers.Verify(m => m.GetByOrderIdAsync(orderPlaced.AggregateId, It.IsAny<CancellationToken>()), Times.Once);
        repository.Verify(r => r.SaveAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Once);
        publisher.Verify(p => p.PublishAsync(It.IsAny<PaymentProcessed>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}
