namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Business policy configuration for payment processing, bound from the <c>PaymentPolicy</c>
/// configuration section. Domain itself never reads configuration directly — this is the
/// Service/host-layer boundary that resolves a plain value and passes it into
/// <c>Payment.Evaluate(decimal)</c>.
/// </summary>
public sealed class PaymentPolicyOptions
{
    public const string SectionName = "PaymentPolicy";

    /// <summary>
    /// Orders with an amount above this threshold are recorded as failed rather than processed
    /// automatically. Not a fixed constant — this is a business-configurable value. Defaults to
    /// <see cref="decimal.MaxValue"/> (no effective threshold) rather than <c>0</c> — a missing
    /// or unconfigured <c>PaymentPolicy</c> section must never silently fail every payment.
    /// </summary>
    public decimal MaxAmountThreshold { get; set; } = decimal.MaxValue;
}
