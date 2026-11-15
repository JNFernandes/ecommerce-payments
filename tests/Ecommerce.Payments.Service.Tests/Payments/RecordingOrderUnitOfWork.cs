using Ecommerce.Payments.Service.Orders;

namespace Ecommerce.Payments.Service.Tests.Payments;

/// <summary>
/// Pass-through <see cref="IOrderUnitOfWork"/> for unit tests: no database, but records
/// <c>Begin:{orderId}</c>, <c>Commit</c> and <c>Dispose</c> into <see cref="Calls"/> (which tests
/// may share with their own mocks to assert ordering).
/// </summary>
internal sealed class RecordingOrderUnitOfWork : IOrderUnitOfWork
{
    public RecordingOrderUnitOfWork(List<string>? calls = null)
    {
        Calls = calls ?? [];
    }

    public List<string> Calls { get; }

    public int Commits => Calls.Count(c => c == "Commit");

    public Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken cancellationToken)
    {
        Calls.Add($"Begin:{orderId}");
        return Task.FromResult<IOrderTransaction>(new Transaction(Calls));
    }

    private sealed class Transaction(List<string> calls) : IOrderTransaction
    {
        public Task CommitAsync(CancellationToken cancellationToken)
        {
            calls.Add("Commit");
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            calls.Add("Dispose");
            return ValueTask.CompletedTask;
        }
    }
}
