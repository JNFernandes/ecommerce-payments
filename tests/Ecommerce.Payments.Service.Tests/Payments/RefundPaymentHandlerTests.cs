using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Payments;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace Ecommerce.Payments.Service.Tests.Payments;

public class RefundPaymentHandlerTests
{
    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 12, 10, 5, 0, TimeSpan.Zero));
    private readonly Mock<IPaymentRepository> _repository = new();
    private readonly Mock<IPaymentEventPublisher> _publisher = new();

    private static OrderCancelledEvent Cancellation(Guid orderId) => new()
    {
        EventId = Guid.NewGuid(),
        OccurredAt = DateTimeOffset.UtcNow,
        AggregateId = orderId,
        Version = 1,
        CustomerId = Guid.NewGuid()
    };

    private static Payment StoredPayment(
        Guid orderId,
        PaymentStatus status,
        Guid? refundSourceEventId = null) =>
        Payment.Rehydrate(
            Guid.NewGuid(), orderId, Guid.NewGuid(), 129.99m, "USD", status, Guid.NewGuid(),
            DateTimeOffset.UtcNow.AddHours(-1),
            processedAt: status is PaymentStatus.Processed or PaymentStatus.Refunded ? DateTimeOffset.UtcNow.AddMinutes(-30) : null,
            failureReason: status == PaymentStatus.Failed ? "over threshold" : null,
            failedAt: status == PaymentStatus.Failed ? DateTimeOffset.UtcNow.AddMinutes(-30) : null,
            refundedAt: status == PaymentStatus.Refunded ? DateTimeOffset.UtcNow.AddMinutes(-1) : null,
            refundSourceEventId: refundSourceEventId);

    private void Stored(Payment? payment) =>
        _repository
            .Setup(r => r.GetByOrderIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(payment);

    private RefundPaymentHandler CreateHandler(ILogger<RefundPaymentHandler>? logger = null) =>
        new(_repository.Object, _publisher.Object, _clock, logger ?? NullLogger<RefundPaymentHandler>.Instance);

    [Fact]
    public async Task Should_Save_Refund_Before_Publishing_PaymentRefunded()
    {
        var orderId = Guid.NewGuid();
        Stored(StoredPayment(orderId, PaymentStatus.Processed));
        var calls = new List<string>();
        _repository
            .Setup(r => r.SaveRefundAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .Callback(() => calls.Add("Save"))
            .ReturnsAsync(true);
        _publisher
            .Setup(p => p.PublishRefundedAsync(It.IsAny<PaymentRefunded>(), It.IsAny<CancellationToken>()))
            .Callback(() => calls.Add("Publish"))
            .Returns(Task.CompletedTask);

        await CreateHandler().HandleAsync(Cancellation(orderId), CancellationToken.None);

        Assert.Equal(["Save", "Publish"], calls);
    }

    [Fact]
    public async Task Should_Publish_PaymentRefunded_With_Payment_Order_Amount_Currency_And_RefundedAt()
    {
        var orderId = Guid.NewGuid();
        var payment = StoredPayment(orderId, PaymentStatus.Processed);
        Stored(payment);
        _repository.Setup(r => r.SaveRefundAsync(payment, It.IsAny<CancellationToken>())).ReturnsAsync(true);
        PaymentRefunded? published = null;
        _publisher
            .Setup(p => p.PublishRefundedAsync(It.IsAny<PaymentRefunded>(), It.IsAny<CancellationToken>()))
            .Callback<PaymentRefunded, CancellationToken>((e, _) => published = e)
            .Returns(Task.CompletedTask);

        await CreateHandler().HandleAsync(Cancellation(orderId), CancellationToken.None);

        Assert.NotNull(published);
        Assert.Equal(payment.Id, published.PaymentId);
        Assert.Equal(orderId, published.OrderId);
        Assert.Equal(129.99m, published.Amount);
        Assert.Equal("USD", published.Currency);
        Assert.Equal(_clock.GetUtcNow(), published.RefundedAt);
    }

    [Fact]
    public async Task Should_Record_Cancelling_EventId_On_Payment()
    {
        var orderId = Guid.NewGuid();
        var payment = StoredPayment(orderId, PaymentStatus.Processed);
        Stored(payment);
        _repository.Setup(r => r.SaveRefundAsync(payment, It.IsAny<CancellationToken>())).ReturnsAsync(true);
        var cancellation = Cancellation(orderId);

        await CreateHandler().HandleAsync(cancellation, CancellationToken.None);

        Assert.Equal(cancellation.EventId, payment.RefundSourceEventId);
        Assert.Equal(PaymentStatus.Refunded, payment.Status);
        _repository.Verify(
            r => r.SaveRefundAsync(It.Is<Payment>(p => p.RefundSourceEventId == cancellation.EventId), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Should_Ignore_Redelivery_With_Same_EventId_Before_Domain()
    {
        var orderId = Guid.NewGuid();
        var cancellation = Cancellation(orderId);
        Stored(StoredPayment(orderId, PaymentStatus.Refunded, refundSourceEventId: cancellation.EventId));

        await CreateHandler().HandleAsync(cancellation, CancellationToken.None);

        _repository.Verify(r => r.SaveRefundAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Never);
        _publisher.Verify(p => p.PublishRefundedAsync(It.IsAny<PaymentRefunded>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Should_Not_Publish_And_Should_Throw_When_Save_Keeps_Failing()
    {
        var orderId = Guid.NewGuid();
        Stored(StoredPayment(orderId, PaymentStatus.Processed));
        _repository
            .Setup(r => r.SaveRefundAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("database down"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateHandler().HandleAsync(Cancellation(orderId), CancellationToken.None));

        _publisher.Verify(p => p.PublishRefundedAsync(It.IsAny<PaymentRefunded>(), It.IsAny<CancellationToken>()), Times.Never);
        // 1 attempt + 3 retries.
        _repository.Verify(r => r.SaveRefundAsync(It.IsAny<Payment>(), It.IsAny<CancellationToken>()), Times.Exactly(4));
    }

    [Fact]
    public async Task Should_Retry_Transient_Load_Failure()
    {
        var orderId = Guid.NewGuid();
        var payment = StoredPayment(orderId, PaymentStatus.Processed);
        _repository
            .SetupSequence(r => r.GetByOrderIdAsync(orderId, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("transient"))
            .ReturnsAsync(payment);
        _repository.Setup(r => r.SaveRefundAsync(payment, It.IsAny<CancellationToken>())).ReturnsAsync(true);

        await CreateHandler().HandleAsync(Cancellation(orderId), CancellationToken.None);

        _publisher.Verify(p => p.PublishRefundedAsync(It.IsAny<PaymentRefunded>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}
