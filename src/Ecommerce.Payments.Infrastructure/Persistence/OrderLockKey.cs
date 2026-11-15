namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Derives the PostgreSQL advisory-lock key for an order (research R2 of US-05): the 128-bit
/// order id folded to 64 bits by XOR-ing its two halves. Deterministic across instances; a
/// collision only serialises two unrelated orders briefly.
/// </summary>
internal static class OrderLockKey
{
    /// <summary>Returns the advisory-lock key for <paramref name="orderId"/>.</summary>
    public static long For(Guid orderId)
    {
        Span<byte> bytes = stackalloc byte[16];
        orderId.TryWriteBytes(bytes);
        return BitConverter.ToInt64(bytes[..8]) ^ BitConverter.ToInt64(bytes[8..]);
    }
}
