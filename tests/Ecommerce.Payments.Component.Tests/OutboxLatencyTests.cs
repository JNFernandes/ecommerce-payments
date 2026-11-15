using System.Text.Json;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// US-07 User Story 2 (FR-006, SC-002): with a healthy broker and the production relay poll
/// interval, an outbox entry is visible on Kafka under 1 s at p95 and 2 s at most after it was
/// recorded. Measured from the entry's <c>RecordedAt</c> to the broker timestamp of the
/// published message, so consumer start-up and test polling are excluded.
/// </summary>
public class OutboxLatencyTests : IAsyncLifetime
{
    private const int Orders = 20;
    private const string OrderPlacedTopic = "orders.order-placed";
    private const string PaymentProcessedTopic = "payments.payment-processed";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _kafka.StartAsync());
        await using var migrateContext = CreateDbContext();
        await migrateContext.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgres.DisposeAsync();
        await _kafka.DisposeAsync();
    }

    [Fact]
    public async Task Healthy_Path_Publishes_Within_One_Second_At_P95()
    {
        // Warm the output topic so the first measured publish does not pay for topic creation.
        Produce(PaymentProcessedTopic, "warm-up", """{"orderId":"00000000-0000-0000-0000-000000000000"}""");

        await using var provider = BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hostedServices)
        {
            await service.StartAsync(CancellationToken.None);
        }

        Dictionary<Guid, DateTimeOffset> publishedAt;
        try
        {
            var orderIds = new List<Guid>();
            for (var i = 0; i < Orders; i++)
            {
                var orderId = Guid.NewGuid();
                orderIds.Add(orderId);
                Produce(OrderPlacedTopic, orderId.ToString(), RefundFlowTests.OrderPlacedJson(orderId));
                await Task.Delay(100);
            }

            publishedAt = ConsumeBrokerTimestamps(orderIds.ToHashSet(), TimeSpan.FromSeconds(60));
        }
        finally
        {
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None);
            }
        }

        Assert.Equal(Orders, publishedAt.Count);

        await using var dbContext = CreateDbContext();
        var recorded = await (
            from entry in dbContext.PaymentOutbox
            join payment in dbContext.Payments on entry.PaymentId equals payment.Id
            select new { payment.OrderId, entry.RecordedAt })
            .ToListAsync();

        var latenciesMs = recorded
            .Select(r => (publishedAt[r.OrderId] - r.RecordedAt).TotalMilliseconds)
            .Order()
            .ToList();
        var p95 = latenciesMs[(int)Math.Ceiling(latenciesMs.Count * 0.95) - 1];

        Assert.True(p95 < 1_000, $"p95 latency {p95:F0} ms (all: {string.Join(", ", latenciesMs.Select(l => l.ToString("F0", System.Globalization.CultureInfo.InvariantCulture)))})");
        Assert.True(latenciesMs[^1] < 2_000, $"max latency {latenciesMs[^1]:F0} ms");
    }

    private Dictionary<Guid, DateTimeOffset> ConsumeBrokerTimestamps(HashSet<Guid> orderIds, TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(PaymentProcessedTopic);

        var result = new Dictionary<Guid, DateTimeOffset>();
        var deadline = DateTime.UtcNow + timeout;
        while (result.Count < orderIds.Count && DateTime.UtcNow < deadline)
        {
            var message = consumer.Consume(TimeSpan.FromSeconds(1));
            if (message is null)
            {
                continue;
            }

            var orderId = JsonDocument.Parse(message.Message.Value).RootElement.GetProperty("orderId").GetGuid();
            if (orderIds.Contains(orderId))
            {
                result[orderId] = message.Message.Timestamp.UtcDateTime;
            }
        }

        return result;
    }

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        services.AddScoped<ProcessPaymentHandler>();
        services.AddOptions<PaymentPolicyOptions>().Configure(o => o.MaxAmountThreshold = 10_000m);
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = _kafka.GetBootstrapAddress();
                o.ConsumerGroupId = $"component-tests-{Guid.NewGuid()}";
                o.OrderPlacedTopic = OrderPlacedTopic;
                o.PaymentProcessedTopic = PaymentProcessedTopic;
            });
        // Production poll interval, not the fast test one.
        var production = new OutboxRelayOptions();
        services.AddOutboxRelay(configure: o => o.PollIntervalMs = production.PollIntervalMs);
        services.AddSingleton<IHostedService, OrderPlacedConsumer>();
        return services.BuildServiceProvider();
    }

    private void Produce(string topic, string key, string json)
    {
        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        producer.Produce(topic, new Message<string, string> { Key = key, Value = json });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
