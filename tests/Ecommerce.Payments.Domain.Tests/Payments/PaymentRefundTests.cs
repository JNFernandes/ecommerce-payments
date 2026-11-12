using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Domain.Tests.Payments;

public class PaymentRefundTests
{
    private static readonly DateTimeOffset RefundTime = new(2026, 11, 12, 10, 5, 0, TimeSpan.Zero);

    private static Payment ProcessedPayment()
    {
        var payment = Payment.CreatePending(Guid.NewGuid(), Guid.NewGuid(), 129.99m, "USD", Guid.NewGuid());
        payment.Process();
        return payment;
    }

    private static Payment PaymentIn(PaymentStatus status) => status switch
    {
        PaymentStatus.Pending => Payment.CreatePending(Guid.NewGuid(), Guid.NewGuid(), 129.99m, "USD", Guid.NewGuid()),
        PaymentStatus.Processed => ProcessedPayment(),
        PaymentStatus.Failed => FailedPayment(),
        PaymentStatus.Refunded => RefundedPayment(),
        _ => throw new ArgumentOutOfRangeException(nameof(status))
    };

    private static Payment FailedPayment()
    {
        var payment = Payment.CreatePending(Guid.NewGuid(), Guid.NewGuid(), 129.99m, "USD", Guid.NewGuid());
        payment.Fail("over threshold");
        return payment;
    }

    private static Payment RefundedPayment()
    {
        var payment = ProcessedPayment();
        payment.Refund(Guid.NewGuid(), RefundTime.AddMinutes(-1));
        return payment;
    }

    [Fact]
    public void Refund_Should_Move_Processed_To_Refunded_And_Return_PaymentRefunded()
    {
        var payment = ProcessedPayment();
        var cancellationEventId = Guid.NewGuid();

        var refunded = payment.Refund(cancellationEventId, RefundTime);

        Assert.Equal(PaymentStatus.Refunded, payment.Status);
        Assert.Equal(RefundTime, payment.RefundedAt);
        Assert.Equal(cancellationEventId, payment.RefundSourceEventId);
        Assert.Equal(payment.Id, refunded.PaymentId);
        Assert.Equal(payment.OrderId, refunded.OrderId);
        Assert.Equal(129.99m, refunded.Amount);
        Assert.Equal("USD", refunded.Currency);
        Assert.Equal(RefundTime, refunded.RefundedAt);
    }

    [Theory]
    [InlineData(PaymentStatus.Pending)]
    [InlineData(PaymentStatus.Failed)]
    [InlineData(PaymentStatus.Refunded)]
    public void Refund_Should_Throw_And_Leave_State_Unchanged_When_Not_Processed(PaymentStatus status)
    {
        var payment = PaymentIn(status);
        var (refundedAt, refundSource) = (payment.RefundedAt, payment.RefundSourceEventId);

        Assert.Throws<InvalidPaymentTransitionException>(() => payment.Refund(Guid.NewGuid(), RefundTime));

        Assert.Equal(status, payment.Status);
        Assert.Equal(refundedAt, payment.RefundedAt);
        Assert.Equal(refundSource, payment.RefundSourceEventId);
    }

    [Fact]
    public void RequestRefund_Should_Return_Refunded_Decision_For_Processed_Payment()
    {
        var payment = ProcessedPayment();

        var decision = payment.RequestRefund(Guid.NewGuid(), RefundTime);

        var refunded = Assert.IsType<RefundDecision.Refunded>(decision);
        Assert.Equal(payment.Id, refunded.Event.PaymentId);
        Assert.Equal(PaymentStatus.Refunded, payment.Status);
    }

    [Theory]
    [InlineData(PaymentStatus.Failed, RefundIgnoredReason.NotCompleted)]
    [InlineData(PaymentStatus.Pending, RefundIgnoredReason.NotCompleted)]
    [InlineData(PaymentStatus.Refunded, RefundIgnoredReason.AlreadyRefunded)]
    public void RequestRefund_Should_Ignore_Non_Processed_Payment_Without_Changing_It(
        PaymentStatus status,
        RefundIgnoredReason expectedReason)
    {
        var payment = PaymentIn(status);
        var (refundedAt, refundSource) = (payment.RefundedAt, payment.RefundSourceEventId);

        var decision = payment.RequestRefund(Guid.NewGuid(), RefundTime);

        var ignored = Assert.IsType<RefundDecision.Ignored>(decision);
        Assert.Equal(expectedReason, ignored.Reason);
        Assert.Equal(status, payment.Status);
        Assert.Equal(refundedAt, payment.RefundedAt);
        Assert.Equal(refundSource, payment.RefundSourceEventId);
    }

    [Fact]
    public void Rehydrate_Should_Restore_Every_Field()
    {
        var (id, orderId, customerId, sourceEventId, refundSource) =
            (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        var createdAt = RefundTime.AddHours(-2);
        var processedAt = RefundTime.AddHours(-1);

        var payment = Payment.Rehydrate(
            id, orderId, customerId, 42.5m, "EUR", PaymentStatus.Refunded, sourceEventId,
            createdAt, processedAt, failureReason: null, failedAt: null, RefundTime, refundSource);

        Assert.Equal(id, payment.Id);
        Assert.Equal(orderId, payment.OrderId);
        Assert.Equal(customerId, payment.CustomerId);
        Assert.Equal(42.5m, payment.Amount);
        Assert.Equal("EUR", payment.Currency);
        Assert.Equal(PaymentStatus.Refunded, payment.Status);
        Assert.Equal(sourceEventId, payment.SourceEventId);
        Assert.Equal(createdAt, payment.CreatedAt);
        Assert.Equal(processedAt, payment.ProcessedAt);
        Assert.Null(payment.FailureReason);
        Assert.Null(payment.FailedAt);
        Assert.Equal(RefundTime, payment.RefundedAt);
        Assert.Equal(refundSource, payment.RefundSourceEventId);
    }
}
