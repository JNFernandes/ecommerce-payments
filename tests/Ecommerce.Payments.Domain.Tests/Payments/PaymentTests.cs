using Ecommerce.Payments.Domain.Payments;

namespace Ecommerce.Payments.Domain.Tests.Payments;

public class PaymentTests
{
    private static (Guid OrderId, Guid CustomerId, decimal Amount, string Currency, Guid SourceEventId) ValidArgs() =>
        (Guid.NewGuid(), Guid.NewGuid(), 129.99m, "USD", Guid.NewGuid());

    [Fact]
    public void Create_ValidArguments_ReturnsNewPaymentInPendingStatus()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();

        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);

        Assert.NotEqual(Guid.Empty, payment.Id);
        Assert.Equal(orderId, payment.OrderId);
        Assert.Equal(customerId, payment.CustomerId);
        Assert.Equal(amount, payment.Amount);
        Assert.Equal(currency, payment.Currency);
        Assert.Equal(sourceEventId, payment.SourceEventId);
        Assert.Equal(PaymentStatus.Pending, payment.Status);
        Assert.Null(payment.ProcessedAt);
        Assert.NotEqual(default, payment.CreatedAt);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Create_NonPositiveAmount_ThrowsArgumentException(decimal amount)
    {
        var (orderId, customerId, _, currency, sourceEventId) = ValidArgs();

        Assert.Throws<ArgumentException>(() =>
            Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId));
    }

    [Theory]
    [InlineData("")]
    [InlineData("US")]
    [InlineData("USDD")]
    public void Create_MalformedCurrency_ThrowsArgumentException(string currency)
    {
        var (orderId, customerId, amount, _, sourceEventId) = ValidArgs();

        Assert.Throws<ArgumentException>(() =>
            Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId));
    }

    [Fact]
    public void Process_PendingPayment_TransitionsToProcessedAndRaisesDomainEvent()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);

        var domainEvent = payment.Process();

        Assert.Equal(PaymentStatus.Processed, payment.Status);
        Assert.NotNull(payment.ProcessedAt);
        Assert.Equal(payment.Id, domainEvent.PaymentId);
        Assert.Equal(orderId, domainEvent.OrderId);
        Assert.Equal(amount, domainEvent.Amount);
        Assert.Equal(currency, domainEvent.Currency);
        Assert.Equal(payment.ProcessedAt, domainEvent.ProcessedAt);
    }

    [Fact]
    public void Process_AlreadyProcessedPayment_ThrowsInvalidPaymentTransitionExceptionWithoutMutatingState()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);
        payment.Process();
        var processedAtAfterFirstProcess = payment.ProcessedAt;

        Assert.Throws<InvalidPaymentTransitionException>(() => payment.Process());

        Assert.Equal(PaymentStatus.Processed, payment.Status);
        Assert.Equal(processedAtAfterFirstProcess, payment.ProcessedAt);
    }

    [Fact]
    public void Fail_PendingPayment_TransitionsToFailedAndRaisesDomainEvent()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);
        const string reason = "Amount exceeds the maximum allowed threshold.";

        var domainEvent = payment.Fail(reason);

        Assert.Equal(PaymentStatus.Failed, payment.Status);
        Assert.Equal(reason, payment.FailureReason);
        Assert.NotNull(payment.FailedAt);
        Assert.Null(payment.ProcessedAt);
        Assert.Equal(payment.Id, domainEvent.PaymentId);
        Assert.Equal(orderId, domainEvent.OrderId);
        Assert.Equal(reason, domainEvent.Reason);
        Assert.Equal(payment.FailedAt, domainEvent.FailedAt);
    }

    [Fact]
    public void Fail_AlreadyProcessedPayment_ThrowsInvalidPaymentTransitionExceptionWithoutMutatingState()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);
        payment.Process();

        Assert.Throws<InvalidPaymentTransitionException>(() => payment.Fail("some reason"));

        Assert.Equal(PaymentStatus.Processed, payment.Status);
        Assert.Null(payment.FailureReason);
        Assert.Null(payment.FailedAt);
    }

    [Fact]
    public void Fail_AlreadyFailedPayment_ThrowsInvalidPaymentTransitionExceptionWithoutMutatingState()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);
        payment.Fail("first reason");
        var failedAtAfterFirstFail = payment.FailedAt;

        Assert.Throws<InvalidPaymentTransitionException>(() => payment.Fail("second reason"));

        Assert.Equal(PaymentStatus.Failed, payment.Status);
        Assert.Equal("first reason", payment.FailureReason);
        Assert.Equal(failedAtAfterFirstFail, payment.FailedAt);
    }

    [Fact]
    public void Evaluate_AmountAtOrBelowThreshold_TransitionsToProcessedAndReturnsPaymentProcessed()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);

        var domainEvent = payment.Evaluate(maxAmountThreshold: amount);

        Assert.Equal(PaymentStatus.Processed, payment.Status);
        Assert.IsType<PaymentProcessed>(domainEvent);
        Assert.Null(payment.FailureReason);
    }

    [Fact]
    public void Evaluate_AmountAboveThreshold_TransitionsToFailedAndReturnsPaymentFailedWithReason()
    {
        var (orderId, customerId, amount, currency, sourceEventId) = ValidArgs();
        var payment = Payment.CreatePending(orderId, customerId, amount, currency, sourceEventId);
        var threshold = amount - 1m;

        var domainEvent = payment.Evaluate(maxAmountThreshold: threshold);

        Assert.Equal(PaymentStatus.Failed, payment.Status);
        var failed = Assert.IsType<PaymentFailed>(domainEvent);
        Assert.Contains(threshold.ToString(System.Globalization.CultureInfo.InvariantCulture), failed.Reason, StringComparison.Ordinal);
        Assert.Null(payment.ProcessedAt);
    }
}
