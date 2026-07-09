namespace Ecommerce.Payments.Domain.Payments;

/// <summary>
/// Aggregate root representing a single charge attempt tied to one order.
/// The only way a <see cref="Payment"/> comes into existence is <see cref="CreatePending"/>,
/// which returns a new instance in <see cref="PaymentStatus.Pending"/>. State is only ever
/// mutated through aggregate methods (e.g. <see cref="Process"/>) — there are no public setters.
/// </summary>
public sealed class Payment
{
    private Payment(
        Guid id,
        Guid orderId,
        Guid customerId,
        decimal amount,
        string currency,
        PaymentStatus status,
        Guid sourceEventId,
        DateTimeOffset createdAt,
        DateTimeOffset? processedAt)
    {
        Id = id;
        OrderId = orderId;
        CustomerId = customerId;
        Amount = amount;
        Currency = currency;
        Status = status;
        SourceEventId = sourceEventId;
        CreatedAt = createdAt;
        ProcessedAt = processedAt;
    }

    /// <summary>Payment aggregate id (<c>aggregateId</c> on outbound events).</summary>
    public Guid Id { get; }

    /// <summary>The order this payment is for. Unique — enforces idempotency.</summary>
    public Guid OrderId { get; }

    /// <summary>Customer being charged.</summary>
    public Guid CustomerId { get; }

    /// <summary>Charge amount, taken as-is from <c>OrderPlaced</c>.</summary>
    public decimal Amount { get; }

    /// <summary>ISO 4217 currency code, taken as-is from <c>OrderPlaced</c>.</summary>
    public string Currency { get; }

    /// <summary>Current lifecycle state of the payment.</summary>
    public PaymentStatus Status { get; private set; }

    /// <summary>The <c>OrderPlaced</c> event's <c>eventId</c>, kept for traceability/log correlation.</summary>
    public Guid SourceEventId { get; }

    /// <summary>When the payment record was first created (<see cref="PaymentStatus.Pending"/>).</summary>
    public DateTimeOffset CreatedAt { get; }

    /// <summary>When the transition to <see cref="PaymentStatus.Processed"/> completed. Null while pending.</summary>
    public DateTimeOffset? ProcessedAt { get; private set; }

    /// <summary>
    /// Creates a new <see cref="Payment"/> in <see cref="PaymentStatus.Pending"/> for the given
    /// order. Validates <paramref name="amount"/> and <paramref name="currency"/> at construction
    /// time — these are immutable afterward, so this is the only place they are checked.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="amount"/> is not greater than zero, or <paramref name="currency"/> is not
    /// a 3-letter code.
    /// </exception>
    public static Payment CreatePending(Guid orderId, Guid customerId, decimal amount, string currency, Guid sourceEventId)
    {
        if (amount <= 0)
        {
            throw new ArgumentException("Amount must be greater than zero.", nameof(amount));
        }

        if (currency is null || currency.Length != 3)
        {
            throw new ArgumentException("Currency must be a 3-letter ISO 4217 code.", nameof(currency));
        }

        return new Payment(
            id: Guid.NewGuid(),
            orderId: orderId,
            customerId: customerId,
            amount: amount,
            currency: currency,
            status: PaymentStatus.Pending,
            sourceEventId: sourceEventId,
            createdAt: DateTimeOffset.UtcNow,
            processedAt: null);
    }

    /// <summary>
    /// Transitions this payment from <see cref="PaymentStatus.Pending"/> to
    /// <see cref="PaymentStatus.Processed"/> and returns the resulting domain event.
    /// </summary>
    /// <exception cref="InvalidPaymentTransitionException">
    /// The payment is not currently <see cref="PaymentStatus.Pending"/>. State is left unchanged.
    /// </exception>
    public PaymentProcessed Process()
    {
        if (Status != PaymentStatus.Pending)
        {
            throw new InvalidPaymentTransitionException(
                $"Cannot process payment {Id} because it is in status {Status}, not {PaymentStatus.Pending}.");
        }

        ProcessedAt = DateTimeOffset.UtcNow;
        Status = PaymentStatus.Processed;

        return new PaymentProcessed(Id, OrderId, Amount, Currency, ProcessedAt.Value);
    }
}
