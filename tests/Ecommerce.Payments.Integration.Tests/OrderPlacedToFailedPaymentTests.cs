using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

public class OrderPlacedToFailedPaymentTests : IAsyncLifetime
{
    private const decimal MaxAmountThreshold = 100m;
    private const decimal AboveThresholdAmount = 15000.00m;

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        var options = new DbContextOptionsBuilder<PaymentsDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;
        await using var migrateContext = new PaymentsDbContext(options);
        await migrateContext.Database.MigrateAsync();
    }

    public async Task DisposeAsync() => await _postgres.DisposeAsync();

    [Fact]
    public async Task HandleMessageAsync_AmountAboveThreshold_PersistsFailedPaymentRowWithReason()
    {
        var provider = BuildServiceProvider();
        using var consumer = CreateConsumer(provider);
        var orderId = Guid.NewGuid();

        var committed = await consumer.HandleMessageAsync(BuildOrderPlacedJson(orderId), CancellationToken.None);

        Assert.True(committed);

        await using var dbContext = provider.GetRequiredService<PaymentsDbContext>();
        var saved = await dbContext.Payments.SingleAsync(p => p.OrderId == orderId);
        Assert.Equal(PaymentStatus.Failed, saved.Status);
        Assert.False(string.IsNullOrWhiteSpace(saved.FailureReason));
        Assert.NotNull(saved.FailedAt);
        Assert.Null(saved.ProcessedAt);

        var publisher = (FakePaymentEventPublisher)provider.GetRequiredService<IPaymentEventPublisher>();
        Assert.Single(publisher.PublishedFailures, p => p.OrderId == orderId);
        Assert.Empty(publisher.Published);
    }

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddSingleton<IPaymentEventPublisher, FakePaymentEventPublisher>();
        services.AddScoped<ProcessPaymentHandler>();
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = "localhost:9092";
                o.ConsumerGroupId = "integration-tests";
                o.OrderPlacedTopic = "orders.order-placed";
                o.PaymentProcessedTopic = "payments.payment-processed";
                o.PaymentFailedTopic = "payments.payment-failed";
            });
        services
            .AddOptions<PaymentPolicyOptions>()
            .Configure(o => o.MaxAmountThreshold = MaxAmountThreshold);

        return services.BuildServiceProvider();
    }

    private static OrderPlacedConsumer CreateConsumer(ServiceProvider provider) => new(
        provider.GetRequiredService<IOptions<KafkaOptions>>(),
        provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<ILogger<OrderPlacedConsumer>>());

    private static string BuildOrderPlacedJson(Guid orderId) => $$"""
        {
          "eventId": "{{Guid.NewGuid()}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "totalAmount": {{AboveThresholdAmount}}
        }
        """;
}
