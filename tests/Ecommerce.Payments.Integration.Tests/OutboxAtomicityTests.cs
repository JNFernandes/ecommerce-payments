using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.IntegrationEvents;
using Ecommerce.Payments.Service.Orders;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// US-07 FR-001 against a real PostgreSQL container: a payment change and its outbox entry are
/// one atomic write — both commit, or (when the commit fails) neither exists.
/// </summary>
public class OutboxAtomicityTests : IAsyncLifetime
{
    private const decimal MaxAmountThreshold = 100m;

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var context = CreateDbContext();
        await context.Database.MigrateAsync();
    }

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Payment_And_Outbox_Entry_Commit_Together()
    {
        await using var provider = BuildServiceProvider();
        var orderPlaced = OrderPlaced(50m);

        await HandleAsync(provider, orderPlaced);

        await using var context = CreateDbContext();
        var payment = await context.Payments.AsNoTracking().SingleAsync(p => p.OrderId == orderPlaced.AggregateId);
        var entry = await context.PaymentOutbox.AsNoTracking().SingleAsync(e => e.PaymentId == payment.Id);
        Assert.Equal(OutboxStatus.Pending, entry.Status);
        Assert.Equal("PaymentProcessedEvent", entry.EventType);
        Assert.Equal("payments.payment-processed", entry.Topic);
        Assert.Equal(payment.Id.ToString(), entry.MessageKey);
        Assert.Contains(entry.EventId.ToString(), entry.Payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Failed_Commit_Leaves_Neither_Payment_Nor_Entry()
    {
        await using var provider = BuildServiceProvider(failCommits: true);
        var orderPlaced = OrderPlaced(50m);

        await Assert.ThrowsAsync<InvalidOperationException>(() => HandleAsync(provider, orderPlaced));

        await using var context = CreateDbContext();
        Assert.False(await context.Payments.AnyAsync(p => p.OrderId == orderPlaced.AggregateId));
        Assert.Equal(0, await context.PaymentOutbox.CountAsync());
    }

    [Fact]
    public async Task Refund_And_Its_Entry_Commit_Together()
    {
        await using var provider = BuildServiceProvider();
        var orderPlaced = OrderPlaced(50m);
        await HandleAsync(provider, orderPlaced);

        await using (var scope = provider.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<RefundPaymentHandler>()
                .HandleAsync(OrderCancelled(orderPlaced.AggregateId), CancellationToken.None);
        }

        await using var context = CreateDbContext();
        var payment = await context.Payments.AsNoTracking().SingleAsync(p => p.OrderId == orderPlaced.AggregateId);
        Assert.Equal(PaymentStatus.Refunded, payment.Status);
        var types = await context.PaymentOutbox.AsNoTracking()
            .Where(e => e.PaymentId == payment.Id).OrderBy(e => e.Sequence).Select(e => e.EventType).ToListAsync();
        Assert.Equal(["PaymentProcessedEvent", "PaymentRefundedEvent"], types);
    }

    [Fact]
    public async Task Ignored_Cancellation_Adds_No_Entry()
    {
        await using var provider = BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            // No payment: a cancellation marker is recorded, no event.
            await scope.ServiceProvider.GetRequiredService<RefundPaymentHandler>()
                .HandleAsync(OrderCancelled(Guid.NewGuid()), CancellationToken.None);
        }

        await using var context = CreateDbContext();
        Assert.Equal(0, await context.PaymentOutbox.CountAsync());
    }

    private static async Task HandleAsync(ServiceProvider provider, OrderPlacedEvent orderPlaced)
    {
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<ProcessPaymentHandler>().HandleAsync(orderPlaced, CancellationToken.None);
    }

    private static OrderPlacedEvent OrderPlaced(decimal amount) => new()
    {
        EventId = Guid.NewGuid(),
        OccurredAt = DateTimeOffset.UtcNow,
        AggregateId = Guid.NewGuid(),
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

    private ServiceProvider BuildServiceProvider(bool failCommits = false)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        if (failCommits)
        {
            services.AddScoped<IOrderUnitOfWork>(sp => new FailingCommitUnitOfWork(
                ActivatorUtilities.CreateInstance<OrderUnitOfWork>(sp)));
        }

        services.AddPaymentOutbox();
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<RefundPaymentHandler>();
        services.AddOptions<PaymentPolicyOptions>().Configure(o => o.MaxAmountThreshold = MaxAmountThreshold);
        services.AddOptions<Infrastructure.Messaging.KafkaOptions>().Configure(o =>
        {
            o.PaymentProcessedTopic = "payments.payment-processed";
            o.PaymentRefundedTopic = "payments.payment-refunded";
        });
        return services.BuildServiceProvider();
    }

    /// <summary>Runs the real transaction but fails every commit, so it always rolls back.</summary>
    private sealed class FailingCommitUnitOfWork(IOrderUnitOfWork inner) : IOrderUnitOfWork
    {
        public async Task<IOrderTransaction> BeginAsync(Guid orderId, CancellationToken cancellationToken) =>
            new FailingTransaction(await inner.BeginAsync(orderId, cancellationToken));

        private sealed class FailingTransaction(IOrderTransaction inner) : IOrderTransaction
        {
            public Task CommitAsync(CancellationToken cancellationToken) =>
                throw new InvalidOperationException("Simulated commit failure");

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }
}
