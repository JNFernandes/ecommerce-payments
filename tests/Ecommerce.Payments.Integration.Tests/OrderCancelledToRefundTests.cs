using System.Globalization;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Consumer → Repository for <c>OrderCancelled</c> (US-04) against a real PostgreSQL container:
/// the consumed message results in the correct persisted <c>Payment</c> state. Publishing is
/// recorded by <see cref="FakePaymentEventPublisher"/>.
/// </summary>
public class OrderCancelledToRefundTests : IAsyncLifetime
{
    private const decimal MaxAmountThreshold = 100m;
    private const decimal ProcessedAmount = 50m;
    private const decimal FailedAmount = 500m;

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private ServiceProvider _provider = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using (var migrateContext = CreateDbContext())
        {
            await migrateContext.Database.MigrateAsync();
        }

        _provider = BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        await _provider.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    private FakePaymentEventPublisher Publisher =>
        (FakePaymentEventPublisher)_provider.GetRequiredService<IPaymentEventPublisher>();

    [Fact]
    public async Task HandleMessageAsync_ProcessedPayment_BecomesRefunded()
    {
        var orderId = Guid.NewGuid();
        await PlaceOrderAsync(orderId, ProcessedAmount);
        var cancellationEventId = Guid.NewGuid();

        using var consumer = CreateCancelledConsumer();
        var committed = await consumer.HandleMessageAsync(OrderCancelledJson(orderId, cancellationEventId), CancellationToken.None);

        Assert.True(committed);
        var row = await LoadAsync(orderId);
        Assert.Equal(PaymentStatus.Refunded, row.Status);
        Assert.NotNull(row.RefundedAt);
        Assert.Equal(cancellationEventId, row.RefundSourceEventId);
        var refund = Assert.Single(Publisher.PublishedRefunds);
        Assert.Equal(row.Id, refund.PaymentId);
        Assert.Equal(orderId, refund.OrderId);
        Assert.Equal(ProcessedAmount, refund.Amount);
    }

    private async Task PlaceOrderAsync(Guid orderId, decimal amount)
    {
        using var consumer = new OrderPlacedConsumer(
            _provider.GetRequiredService<IOptions<KafkaOptions>>(),
            _provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<ILogger<OrderPlacedConsumer>>());
        Assert.True(await consumer.HandleMessageAsync(OrderPlacedJson(orderId, amount), CancellationToken.None));
    }

    private OrderCancelledConsumer CreateCancelledConsumer() => new(
        _provider.GetRequiredService<IOptions<KafkaOptions>>(),
        _provider.GetRequiredService<IServiceScopeFactory>(),
        _provider.GetRequiredService<ILogger<OrderCancelledConsumer>>());

    private async Task<PaymentEntity> LoadAsync(Guid orderId)
    {
        await using var dbContext = CreateDbContext();
        return await dbContext.Payments.AsNoTracking().SingleAsync(p => p.OrderId == orderId);
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddSingleton<IPaymentEventPublisher, FakePaymentEventPublisher>();
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<RefundPaymentHandler>();
        services.AddOptions<PaymentPolicyOptions>().Configure(o => o.MaxAmountThreshold = MaxAmountThreshold);
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = "localhost:9092";
                o.ConsumerGroupId = "integration-tests";
            });

        return services.BuildServiceProvider();
    }

    private static string OrderPlacedJson(Guid orderId, decimal amount) => $$"""
        {
          "eventId": "{{Guid.NewGuid()}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "totalAmount": {{amount.ToString(CultureInfo.InvariantCulture)}}
        }
        """;

    internal static string OrderCancelledJson(Guid orderId, Guid eventId) => $$"""
        {
          "eventId": "{{eventId}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}"
        }
        """;
}
