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

    [Fact]
    public async Task HandleMessageAsync_NoPayment_IsCommittedAndChangesNothing()
    {
        using var consumer = CreateCancelledConsumer();

        var committed = await consumer.HandleMessageAsync(OrderCancelledJson(Guid.NewGuid(), Guid.NewGuid()), CancellationToken.None);

        Assert.True(committed);
        await using var dbContext = CreateDbContext();
        Assert.Equal(0, await dbContext.Payments.CountAsync());
        Assert.Empty(Publisher.PublishedRefunds);
    }

    [Fact]
    public async Task HandleMessageAsync_FailedPayment_StaysFailed()
    {
        var orderId = Guid.NewGuid();
        await PlaceOrderAsync(orderId, FailedAmount);
        using var consumer = CreateCancelledConsumer();

        var committed = await consumer.HandleMessageAsync(OrderCancelledJson(orderId, Guid.NewGuid()), CancellationToken.None);

        Assert.True(committed);
        var row = await LoadAsync(orderId);
        Assert.Equal(PaymentStatus.Failed, row.Status);
        Assert.Null(row.RefundedAt);
        Assert.Null(row.RefundSourceEventId);
        Assert.Empty(Publisher.PublishedRefunds);
    }

    [Fact]
    public async Task HandleMessageAsync_SecondCancellation_DoesNotRefundTwice()
    {
        var orderId = Guid.NewGuid();
        await PlaceOrderAsync(orderId, ProcessedAmount);
        var firstEventId = Guid.NewGuid();
        using var consumer = CreateCancelledConsumer();

        Assert.True(await consumer.HandleMessageAsync(OrderCancelledJson(orderId, firstEventId), CancellationToken.None));
        var afterFirst = await LoadAsync(orderId);
        // Same message redelivered, then a re-publish of the cancellation with a new eventId.
        Assert.True(await consumer.HandleMessageAsync(OrderCancelledJson(orderId, firstEventId), CancellationToken.None));
        Assert.True(await consumer.HandleMessageAsync(OrderCancelledJson(orderId, Guid.NewGuid()), CancellationToken.None));

        var row = await LoadAsync(orderId);
        Assert.Equal(PaymentStatus.Refunded, row.Status);
        Assert.Equal(firstEventId, row.RefundSourceEventId);
        Assert.Equal(afterFirst.RefundedAt, row.RefundedAt);
        Assert.Single(Publisher.PublishedRefunds);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{ "eventId": "6f1d2c3b-1111-4000-8000-000000000001", "version": 1 }""")]
    public async Task HandleMessageAsync_MalformedMessage_IsCommitted(string rawMessage)
    {
        using var consumer = CreateCancelledConsumer();

        Assert.True(await consumer.HandleMessageAsync(rawMessage, CancellationToken.None));
        Assert.Empty(Publisher.PublishedRefunds);
    }

    [Fact]
    public async Task SaveRefundAsync_Concurrent_OnlyOneSucceeds()
    {
        var orderId = Guid.NewGuid();
        await PlaceOrderAsync(orderId, ProcessedAmount);

        await using var contextA = CreateDbContext();
        await using var contextB = CreateDbContext();
        var repositoryA = new PaymentRepository(contextA);
        var repositoryB = new PaymentRepository(contextB);
        var paymentA = (await repositoryA.GetByOrderIdAsync(orderId, CancellationToken.None))!;
        var paymentB = (await repositoryB.GetByOrderIdAsync(orderId, CancellationToken.None))!;
        paymentA.Refund(Guid.NewGuid(), DateTimeOffset.UtcNow);
        paymentB.Refund(Guid.NewGuid(), DateTimeOffset.UtcNow);

        var results = await Task.WhenAll(
            repositoryA.SaveRefundAsync(paymentA, CancellationToken.None),
            repositoryB.SaveRefundAsync(paymentB, CancellationToken.None));

        Assert.Single(results, saved => saved);
        Assert.Equal(PaymentStatus.Refunded, (await LoadAsync(orderId)).Status);
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
