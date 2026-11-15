using System.Globalization;
using System.Text.Json;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// US-05 end to end with real Kafka and PostgreSQL: an <c>OrderCancelled</c> that arrives before
/// its <c>OrderPlaced</c> records a marker, and the late <c>OrderPlaced</c> is consumed and
/// committed without charging anything or publishing any payment event.
/// </summary>
public class CancelBeforePlaceFlowTests : IAsyncLifetime
{
    private const string OrderPlacedTopic = "orders.order-placed";
    private const string OrderCancelledTopic = "orders.order-cancelled";
    private const string PaymentProcessedTopic = "payments.payment-processed";
    private const string PaymentFailedTopic = "payments.payment-failed";
    private const string PaymentRefundedTopic = "payments.payment-refunded";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.6.1").Build();
    private readonly string _groupId = $"component-tests-{Guid.NewGuid()}";

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
    public async Task Cancel_Then_Place_Publishes_Nothing_And_Charges_Nothing()
    {
        await using var provider = BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        var orderId = Guid.NewGuid();

        foreach (var service in hostedServices)
        {
            await service.StartAsync(CancellationToken.None);
        }

        try
        {
            await ProduceAsync(OrderCancelledTopic, OrderCancelledJson(orderId));
            await PollForMarkerAsync(orderId);

            var placed = await ProduceAsync(OrderPlacedTopic, OrderPlacedJson(orderId));
            await PollUntilCommittedPastAsync(placed.TopicPartitionOffset);
        }
        finally
        {
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None);
            }
        }

        await using (var dbContext = CreateDbContext())
        {
            Assert.False(await dbContext.Payments.AnyAsync(p => p.OrderId == orderId));
        }

        var orderIds = ConsumeOrderIds([PaymentProcessedTopic, PaymentFailedTopic, PaymentRefundedTopic], TimeSpan.FromSeconds(5));
        Assert.DoesNotContain(orderId, orderIds);
    }

    private async Task PollForMarkerAsync(Guid orderId)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            await using var dbContext = CreateDbContext();
            if (await dbContext.CancellationMarkers.AnyAsync(m => m.OrderId == orderId))
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"No cancellation marker for order {orderId}.");
    }

    /// <summary>Waits until the service's consumer group has committed past <paramref name="produced"/>.</summary>
    private async Task PollUntilCommittedPastAsync(TopicPartitionOffset produced)
    {
        using var probe = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = _groupId
        }).Build();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var committed = probe.Committed([produced.TopicPartition], TimeSpan.FromSeconds(5)).Single();
            if (committed.Offset.Value > produced.Offset.Value)
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"Offset {produced} was not committed.");
    }

    /// <summary>Reads every message on <paramref name="topics"/> until idle and returns their <c>orderId</c>s.</summary>
    private List<Guid> ConsumeOrderIds(IEnumerable<string> topics, TimeSpan idle)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false
        }).Build();
        consumer.Subscribe(topics);

        var orderIds = new List<Guid>();
        var lastMessageAt = DateTime.UtcNow;
        while (DateTime.UtcNow - lastMessageAt < idle)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is not null)
                {
                    orderIds.Add(JsonDocument.Parse(result.Message.Value).RootElement.GetProperty("orderId").GetGuid());
                    lastMessageAt = DateTime.UtcNow;
                }
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                // Topic never created: nothing was published to it.
            }
        }

        return orderIds;
    }

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        services.AddScoped<IPaymentOutboxRepository, PaymentOutboxRepository>();
        services.AddSingleton<PaymentEventPublisher>();
        services.AddSingleton<IPaymentEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
        services.AddSingleton<IRawEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<RefundPaymentHandler>();
        services.AddOptions<PaymentPolicyOptions>().Configure(o => o.MaxAmountThreshold = 10_000m);
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = _kafka.GetBootstrapAddress();
                o.ConsumerGroupId = _groupId;
                o.OrderPlacedTopic = OrderPlacedTopic;
                o.OrderCancelledTopic = OrderCancelledTopic;
                o.PaymentProcessedTopic = PaymentProcessedTopic;
                o.PaymentFailedTopic = PaymentFailedTopic;
                o.PaymentRefundedTopic = PaymentRefundedTopic;
            });
        services.AddSingleton<IHostedService, OrderPlacedConsumer>();
        services.AddSingleton<IHostedService, OrderCancelledConsumer>();

        return services.BuildServiceProvider();
    }

    private async Task<DeliveryResult<Null, string>> ProduceAsync(string topic, string json)
    {
        using var producer = new ProducerBuilder<Null, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        return await producer.ProduceAsync(topic, new Message<Null, string> { Value = json });
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private static string OrderPlacedJson(Guid orderId) => $$"""
        {
          "eventId": "{{Guid.NewGuid()}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "totalAmount": {{129.99m.ToString(CultureInfo.InvariantCulture)}}
        }
        """;

    private static string OrderCancelledJson(Guid orderId) => $$"""
        {
          "eventId": "{{Guid.NewGuid()}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "reason": "Stock rejected"
        }
        """;
}
