using System.Collections.Concurrent;
using Ecommerce.Payments.Domain.Orders;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Orders;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// US-05 User Story 3 on a real PostgreSQL container: an <c>OrderPlaced</c> and an
/// <c>OrderCancelled</c> for the same order handled at the same moment always end in an allowed
/// outcome — a marker and no payment, or a payment that is Failed or Refunded and no marker —
/// thanks to the per-order lock (ADR 0004). A last test removes the lock to show the race is real.
/// </summary>
public sealed class PlaceCancelRaceTests : IAsyncLifetime, IDisposable
{
    private const decimal MaxAmountThreshold = 100m;
    private const decimal ProcessedAmount = 50m;
    private const decimal FailedAmount = 500m;
    private const int Rounds = 50;

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly CapturingLoggerProvider _logs = new();
    private ServiceProvider _provider = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using (var context = CreateDbContext())
        {
            await context.Database.MigrateAsync();
        }

        _provider = BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        await _provider.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    public void Dispose() => _logs.Dispose();

    private OutboxReader Outbox =>
        _provider.GetRequiredService<OutboxReader>();

    [Fact]
    public async Task Concurrent_Place_And_Cancel_Always_End_In_Allowed_Outcome()
    {
        var orders = new List<Guid>();
        for (var round = 0; round < Rounds; round++)
        {
            var orderId = Guid.NewGuid();
            orders.Add(orderId);
            var amount = round % 2 == 0 ? ProcessedAmount : FailedAmount;

            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var place = Task.Run(async () =>
            {
                await start.Task;
                await PlaceAsync(orderId, amount);
            });
            var cancel = Task.Run(async () =>
            {
                await start.Task;
                await CancelAsync(orderId);
            });
            start.SetResult();
            await Task.WhenAll(place, cancel);
        }

        var outcomes = new List<string>();
        foreach (var orderId in orders)
        {
            outcomes.Add(await AssertAllowedOutcomeAsync(orderId));
        }

        Assert.DoesNotContain(_logs.Entries, e => e.Level >= LogLevel.Error);
        // Not an assertion on timing, just visibility of what the rounds exercised.
        Console.WriteLine(string.Join(", ", outcomes.GroupBy(o => o).Select(g => $"{g.Key}={g.Count()}")));
    }

    [Fact]
    public async Task Place_Wins_When_Committed_First_Then_Cancel_Refunds()
    {
        var orderId = Guid.NewGuid();
        await using var placeScope = _provider.CreateAsyncScope();
        var gate = new GatedOrderUnitOfWork(placeScope.ServiceProvider.GetRequiredService<IOrderUnitOfWork>());
        var placeHandler = ActivatorUtilities.CreateInstance<ProcessPaymentHandler>(placeScope.ServiceProvider, gate);

        var place = placeHandler.HandleAsync(OrderPlaced(orderId, ProcessedAmount), CancellationToken.None);
        await gate.LockAcquired.WaitAsync(TimeSpan.FromSeconds(10));
        var cancel = CancelAsync(orderId);

        await Task.Delay(300);
        Assert.False(cancel.IsCompleted);

        gate.ReleaseCommit();
        await Task.WhenAll(place, cancel).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal("refunded", await AssertAllowedOutcomeAsync(orderId));
    }

    [Fact]
    public async Task Cancel_Wins_When_Committed_First_Then_Place_Creates_Nothing()
    {
        var orderId = Guid.NewGuid();
        await using var cancelScope = _provider.CreateAsyncScope();
        var gate = new GatedOrderUnitOfWork(cancelScope.ServiceProvider.GetRequiredService<IOrderUnitOfWork>());
        var cancelHandler = ActivatorUtilities.CreateInstance<RefundPaymentHandler>(cancelScope.ServiceProvider, gate);

        var cancel = cancelHandler.HandleAsync(OrderCancelled(orderId), CancellationToken.None);
        await gate.LockAcquired.WaitAsync(TimeSpan.FromSeconds(10));
        var place = PlaceAsync(orderId, ProcessedAmount);

        await Task.Delay(300);
        Assert.False(place.IsCompleted);

        gate.ReleaseCommit();
        await Task.WhenAll(place, cancel).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal("marker", await AssertAllowedOutcomeAsync(orderId));
    }

    /// <summary>
    /// Demonstrates why the lock exists (research R9): with a plain transaction and both sides
    /// pausing after their last read, each sees neither a payment nor a marker and both commit.
    /// Deterministic because the pause is injected after the reads, not left to scheduling.
    /// </summary>
    [Fact]
    public async Task Without_Order_Lock_The_Race_Leaves_Processed_Payment_With_Marker()
    {
        var orderId = Guid.NewGuid();
        var bothRead = new CountdownEvent(2);

        async Task RunAsync(Func<IServiceProvider, IOrderUnitOfWork, ICancellationMarkerRepository, Task> handle)
        {
            await using var scope = _provider.CreateAsyncScope();
            var sp = scope.ServiceProvider;
            var unlocked = new UnlockedOrderUnitOfWork(sp.GetRequiredService<PaymentsDbContext>());
            var pausingMarkers = new PausingMarkerRepository(sp.GetRequiredService<ICancellationMarkerRepository>(), bothRead);
            await handle(sp, unlocked, pausingMarkers);
        }

        await Task.WhenAll(
            Task.Run(() => RunAsync((sp, uow, markers) =>
                ActivatorUtilities.CreateInstance<ProcessPaymentHandler>(sp, uow, markers)
                    .HandleAsync(OrderPlaced(orderId, ProcessedAmount), CancellationToken.None))),
            Task.Run(() => RunAsync((sp, uow, markers) =>
                ActivatorUtilities.CreateInstance<RefundPaymentHandler>(sp, uow, markers)
                    .HandleAsync(OrderCancelled(orderId), CancellationToken.None))));

        await using var context = CreateDbContext();
        Assert.True(await context.CancellationMarkers.AnyAsync(m => m.OrderId == orderId));
        var payment = await context.Payments.SingleAsync(p => p.OrderId == orderId);
        Assert.Equal(PaymentStatus.Processed, payment.Status);
    }

    /// <summary>Asserts the order ended in an allowed state and returns which one.</summary>
    private async Task<string> AssertAllowedOutcomeAsync(Guid orderId)
    {
        await using var context = CreateDbContext();
        var hasMarker = await context.CancellationMarkers.AnyAsync(m => m.OrderId == orderId);
        var payment = await context.Payments.AsNoTracking().SingleOrDefaultAsync(p => p.OrderId == orderId);
        var refunds = Outbox.Refunded.Count(r => r.OrderId == orderId);

        if (hasMarker)
        {
            Assert.Null(payment);
            Assert.DoesNotContain(Outbox.Processed, e => e.OrderId == orderId);
            Assert.DoesNotContain(Outbox.Failed, e => e.OrderId == orderId);
            Assert.Equal(0, refunds);
            return "marker";
        }

        Assert.NotNull(payment);
        Assert.NotEqual(PaymentStatus.Processed, payment.Status);
        if (payment.Status == PaymentStatus.Refunded)
        {
            Assert.Equal(1, refunds);
            return "refunded";
        }

        Assert.Equal(PaymentStatus.Failed, payment.Status);
        Assert.Equal(0, refunds);
        return "failed";
    }

    private async Task PlaceAsync(Guid orderId, decimal amount)
    {
        await using var scope = _provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<ProcessPaymentHandler>()
            .HandleAsync(OrderPlaced(orderId, amount), CancellationToken.None);
    }

    private async Task CancelAsync(Guid orderId)
    {
        await using var scope = _provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<RefundPaymentHandler>()
            .HandleAsync(OrderCancelled(orderId), CancellationToken.None);
    }

    private static OrderPlacedEvent OrderPlaced(Guid orderId, decimal amount) => new()
    {
        EventId = Guid.NewGuid(),
        OccurredAt = DateTimeOffset.UtcNow,
        AggregateId = orderId,
        Version = 1,
        CustomerId = Guid.NewGuid(),
        TotalAmount = amount
    };

    private static OrderCancelledEvent OrderCancelled(Guid orderId) => new()
    {
        EventId = Guid.NewGuid(),
        OccurredAt = DateTimeOffset.UtcNow,
        AggregateId = orderId,
        Version = 1
    };

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(_logs));
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        services.AddPaymentOutbox();
        services.AddSingleton<OutboxReader>();
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<RefundPaymentHandler>();
        services.AddOptions<PaymentPolicyOptions>().Configure(o => o.MaxAmountThreshold = MaxAmountThreshold);
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Wraps the real unit of work: signals <see cref="LockAcquired"/> once the lock is held and
    /// holds the commit until <see cref="ReleaseCommit"/> is called.
    /// </summary>
    private sealed class GatedOrderUnitOfWork(IOrderUnitOfWork inner) : IOrderUnitOfWork
    {
        private readonly TaskCompletionSource _lockAcquired = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task LockAcquired => _lockAcquired.Task;

        public void ReleaseCommit() => _release.TrySetResult();

        public async Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken cancellationToken)
        {
            var transaction = await inner.BeginAsync(orderId, cancellationToken);
            _lockAcquired.TrySetResult();
            return new GatedTransaction(transaction, _release.Task);
        }

        private sealed class GatedTransaction(IOrderTransaction inner, Task release) : IOrderTransaction
        {
            public async Task CommitAsync(CancellationToken cancellationToken)
            {
                await release;
                await inner.CommitAsync(cancellationToken);
            }

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    /// <summary>A plain transaction with no per-order lock — only for the demonstration test.</summary>
    private sealed class UnlockedOrderUnitOfWork(PaymentsDbContext context) : IOrderUnitOfWork
    {
        public async Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken cancellationToken) =>
            new Transaction(await context.Database.BeginTransactionAsync(cancellationToken));

        private sealed class Transaction(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction inner) : IOrderTransaction
        {
            public Task CommitAsync(CancellationToken cancellationToken) => inner.CommitAsync(cancellationToken);

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    /// <summary>After reading a marker, waits until both handlers have read (forces the race window).</summary>
    private sealed class PausingMarkerRepository(ICancellationMarkerRepository inner, CountdownEvent bothRead)
        : ICancellationMarkerRepository
    {
        public async Task<CancellationMarker?> GetByOrderIdAsync(Guid orderId, CancellationToken cancellationToken)
        {
            var marker = await inner.GetByOrderIdAsync(orderId, cancellationToken);
            bothRead.Signal();
            await Task.Run(() => bothRead.Wait(TimeSpan.FromSeconds(10)), cancellationToken);
            return marker;
        }

        public Task AddAsync(CancellationMarker marker, CancellationToken cancellationToken) =>
            inner.AddAsync(marker, cancellationToken);
    }

    /// <summary>Records every log entry from every category.</summary>
    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public ConcurrentBag<(LogLevel Level, string Category, string Message)> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

        public void Dispose()
        {
        }

        private sealed class Logger(CapturingLoggerProvider owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                owner.Entries.Add((logLevel, category, formatter(state, exception)));
        }
    }
}
