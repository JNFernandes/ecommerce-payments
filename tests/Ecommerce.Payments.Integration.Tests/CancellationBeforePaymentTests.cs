using System.Globalization;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Outbox;
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
/// US-05 against a real PostgreSQL container: an <c>OrderCancelled</c> that arrives before the
/// order's payment records a cancellation marker, and a later <c>OrderPlaced</c> for that order
/// creates no payment and publishes nothing. Publishing is recorded by
/// <see cref="OutboxReader"/>.
/// </summary>
public class CancellationBeforePaymentTests : IAsyncLifetime
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

    private OutboxReader Outbox =>
        _provider.GetRequiredService<OutboxReader>();

    // ---- User Story 1: record the marker ----

    [Fact]
    public async Task OrderCancelled_Without_Payment_Records_One_Marker()
    {
        var orderId = Guid.NewGuid();
        var eventId = Guid.NewGuid();

        Assert.True(await CancelAsync(orderId, eventId, reason: "stock rejected"));

        var marker = Assert.Single(await MarkersAsync(orderId));
        Assert.Equal(eventId, marker.SourceEventId);
        Assert.Equal("stock rejected", marker.Reason);
        Assert.Equal(0, await PaymentCountAsync(orderId));
        Assert.Empty(Outbox.Refunded);
    }

    [Fact]
    public async Task Redelivered_OrderCancelled_Leaves_One_Marker()
    {
        var orderId = Guid.NewGuid();
        var eventId = Guid.NewGuid();

        Assert.True(await CancelAsync(orderId, eventId));
        Assert.True(await CancelAsync(orderId, eventId));

        Assert.Single(await MarkersAsync(orderId));
    }

    [Fact]
    public async Task Second_OrderCancelled_With_New_EventId_Leaves_Original_Marker()
    {
        var orderId = Guid.NewGuid();
        var firstEventId = Guid.NewGuid();

        Assert.True(await CancelAsync(orderId, firstEventId));
        Assert.True(await CancelAsync(orderId, Guid.NewGuid()));

        var marker = Assert.Single(await MarkersAsync(orderId));
        Assert.Equal(firstEventId, marker.SourceEventId);
    }

    [Fact]
    public async Task Malformed_OrderCancelled_Records_No_Marker()
    {
        using var consumer = CreateCancelledConsumer();

        await Assert.ThrowsAsync<InvalidMessageException>(() => consumer.HandleMessageAsync(
            """{ "eventId": "6f1d2c3b-1111-4000-8000-000000000001", "version": 1 }""", CancellationToken.None));

        await using var dbContext = CreateDbContext();
        Assert.Equal(0, await dbContext.CancellationMarkers.CountAsync());
    }

    // ---- User Story 2: do not charge a marked order ----

    [Fact]
    public async Task OrderPlaced_After_Marker_Creates_No_Payment_And_Publishes_Nothing()
    {
        var orderId = Guid.NewGuid();
        Assert.True(await CancelAsync(orderId, Guid.NewGuid()));

        Assert.True(await PlaceAsync(orderId, ProcessedAmount));

        Assert.Equal(0, await PaymentCountAsync(orderId));
        Assert.DoesNotContain(Outbox.Processed, e => e.OrderId == orderId);
        Assert.DoesNotContain(Outbox.Failed, e => e.OrderId == orderId);
    }

    [Fact]
    public async Task Redelivered_OrderPlaced_After_Marker_Still_Creates_Nothing()
    {
        var orderId = Guid.NewGuid();
        var placedEventId = Guid.NewGuid();
        Assert.True(await CancelAsync(orderId, Guid.NewGuid()));

        Assert.True(await PlaceAsync(orderId, FailedAmount, placedEventId));
        Assert.True(await PlaceAsync(orderId, FailedAmount, placedEventId));

        Assert.Equal(0, await PaymentCountAsync(orderId));
        Assert.DoesNotContain(Outbox.Processed, e => e.OrderId == orderId);
        Assert.DoesNotContain(Outbox.Failed, e => e.OrderId == orderId);
    }

    [Fact]
    public async Task OrderPlaced_Then_OrderCancelled_Refunds_And_Records_No_Marker()
    {
        var orderId = Guid.NewGuid();
        Assert.True(await PlaceAsync(orderId, ProcessedAmount));

        Assert.True(await CancelAsync(orderId, Guid.NewGuid()));

        Assert.Equal(PaymentStatus.Refunded, (await LoadPaymentAsync(orderId)).Status);
        Assert.Empty(await MarkersAsync(orderId));
        Assert.Single(Outbox.Refunded, r => r.OrderId == orderId);
    }

    [Fact]
    public async Task Failed_Payment_Then_OrderCancelled_Records_No_Marker()
    {
        var orderId = Guid.NewGuid();
        Assert.True(await PlaceAsync(orderId, FailedAmount));

        Assert.True(await CancelAsync(orderId, Guid.NewGuid()));

        Assert.Equal(PaymentStatus.Failed, (await LoadPaymentAsync(orderId)).Status);
        Assert.Empty(await MarkersAsync(orderId));
        Assert.DoesNotContain(Outbox.Refunded, r => r.OrderId == orderId);
    }

    // ---- Helpers ----

    private async Task<bool> CancelAsync(Guid orderId, Guid eventId, string? reason = null)
    {
        using var consumer = CreateCancelledConsumer();
        return await consumer.HandleMessageAsync(OrderCancelledJson(orderId, eventId, reason), CancellationToken.None);
    }

    private async Task<bool> PlaceAsync(Guid orderId, decimal amount, Guid? eventId = null)
    {
        using var consumer = new OrderPlacedConsumer(
            _provider.GetRequiredService<IOptions<KafkaOptions>>(),
            _provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<ILogger<OrderPlacedConsumer>>());
        return await consumer.HandleMessageAsync(OrderPlacedJson(orderId, amount, eventId ?? Guid.NewGuid()), CancellationToken.None);
    }

    private OrderCancelledConsumer CreateCancelledConsumer() => new(
        _provider.GetRequiredService<IOptions<KafkaOptions>>(),
        _provider.GetRequiredService<IServiceScopeFactory>(),
        _provider.GetRequiredService<ILogger<OrderCancelledConsumer>>());

    private async Task<List<CancellationMarkerEntity>> MarkersAsync(Guid orderId)
    {
        await using var dbContext = CreateDbContext();
        return await dbContext.CancellationMarkers.AsNoTracking().Where(m => m.OrderId == orderId).ToListAsync();
    }

    private async Task<int> PaymentCountAsync(Guid orderId)
    {
        await using var dbContext = CreateDbContext();
        return await dbContext.Payments.CountAsync(p => p.OrderId == orderId);
    }

    private async Task<PaymentEntity> LoadPaymentAsync(Guid orderId)
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
        services.AddOrderPersistence();
        services.AddPaymentOutbox();
        services.AddSingleton<OutboxReader>();
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

    private static string OrderPlacedJson(Guid orderId, decimal amount, Guid eventId) => $$"""
        {
          "eventId": "{{eventId}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "totalAmount": {{amount.ToString(CultureInfo.InvariantCulture)}}
        }
        """;

    private static string OrderCancelledJson(Guid orderId, Guid eventId, string? reason) => $$"""
        {
          "eventId": "{{eventId}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}"{{(reason is null ? "" : $",\n  \"reason\": \"{reason}\"")}}
        }
        """;
}
