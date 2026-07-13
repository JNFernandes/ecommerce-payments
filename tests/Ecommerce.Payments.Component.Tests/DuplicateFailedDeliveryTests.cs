using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Proves the idempotency guarantee built for the success path (US-01) already covers the
/// failure path too — no new production code, only test coverage. See research.md #3.
/// </summary>
public class DuplicateFailedDeliveryTests : IAsyncLifetime
{
    private const string OrderPlacedTopic = "orders.order-placed";
    private const string PaymentFailedTopic = "payments.payment-failed";
    private const decimal MaxAmountThreshold = 100m;
    private const decimal AboveThresholdAmount = 15000.00m;

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _kafka.StartAsync());

        var options = new DbContextOptionsBuilder<PaymentsDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;
        await using var migrateContext = new PaymentsDbContext(options);
        await migrateContext.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgres.DisposeAsync();
        await _kafka.DisposeAsync();
    }

    [Fact]
    public async Task RedeliveringIdenticalAboveThresholdOrderPlaced_ResultsInExactlyOneFailedPaymentAndOnePublish()
    {
        await using var provider = BuildServiceProvider();
        using var consumer = new OrderPlacedConsumer(
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<OrderPlacedConsumer>>());

        var orderId = Guid.NewGuid();
        var rawMessage = BuildOrderPlacedJson(orderId);

        var firstCommit = await consumer.HandleMessageAsync(rawMessage, CancellationToken.None);
        var secondCommit = await consumer.HandleMessageAsync(rawMessage, CancellationToken.None);

        Assert.True(firstCommit);
        Assert.True(secondCommit);

        await using var dbContext = provider.GetRequiredService<PaymentsDbContext>();
        var paymentCount = await dbContext.Payments.CountAsync(p => p.OrderId == orderId);
        Assert.Equal(1, paymentCount);

        var publishedCount = CountPublishedFailuresForOrder(orderId, TimeSpan.FromSeconds(15));
        Assert.Equal(1, publishedCount);
    }

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddSingleton<IPaymentEventPublisher, PaymentEventPublisher>();
        services.AddScoped<ProcessPaymentHandler>();
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = _kafka.GetBootstrapAddress();
                o.ConsumerGroupId = $"component-tests-{Guid.NewGuid()}";
                o.OrderPlacedTopic = OrderPlacedTopic;
                o.PaymentFailedTopic = PaymentFailedTopic;
            });
        services
            .AddOptions<PaymentPolicyOptions>()
            .Configure(o => o.MaxAmountThreshold = MaxAmountThreshold);

        return services.BuildServiceProvider();
    }

    private int CountPublishedFailuresForOrder(Guid orderId, TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(PaymentFailedTopic);
        var count = 0;
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            ConsumeResult<Ignore, string>? result;
            try
            {
                result = consumer.Consume(TimeSpan.FromSeconds(2));
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                // Topic not visible to this consumer yet (auto-create race) — keep polling.
                continue;
            }

            if (result is null)
            {
                continue;
            }

            if (result.Message.Value.Contains(orderId.ToString(), StringComparison.Ordinal))
            {
                count++;
            }
        }

        return count;
    }

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
