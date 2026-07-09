using System.Text.Json;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

public class ProcessPaymentFlowTests : IAsyncLifetime
{
    private const string OrderPlacedTopic = "orders.order-placed";
    private const string PaymentProcessedTopic = "payments.payment-processed";

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
    public async Task FullFlow_ValidOrderPlaced_SavesPaymentAndPublishesPaymentProcessed()
    {
        await using var provider = BuildServiceProvider();
        var consumer = (OrderPlacedConsumer)provider.GetRequiredService<Microsoft.Extensions.Hosting.IHostedService>();

        var orderId = Guid.NewGuid();
        ProduceOrderPlaced(orderId);

        await consumer.StartAsync(CancellationToken.None);
        try
        {
            var payment = await PollForPaymentAsync(provider, orderId, TimeSpan.FromSeconds(30));
            Assert.Equal(PaymentStatus.Processed, payment.Status);

            var publishedMessage = ConsumePaymentProcessed(TimeSpan.FromSeconds(30));
            using var document = JsonDocument.Parse(publishedMessage);
            var root = document.RootElement;

            Assert.Equal(orderId, root.GetProperty("orderId").GetGuid());
            Assert.Equal(payment.Id, root.GetProperty("aggregateId").GetGuid());
            Assert.Equal(129.99m, root.GetProperty("amount").GetDecimal());
            Assert.Equal("USD", root.GetProperty("currency").GetString());
            Assert.True(root.TryGetProperty("eventId", out _));
            Assert.True(root.TryGetProperty("processedAt", out _));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }
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
                o.PaymentProcessedTopic = PaymentProcessedTopic;
            });
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService, OrderPlacedConsumer>();

        return services.BuildServiceProvider();
    }

    private void ProduceOrderPlaced(Guid orderId)
    {
        using var producer = new ProducerBuilder<Null, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();

        // Matches the real payload shape from the orders service (no separate orderId,
        // no currency, amount is "totalAmount") — see OrderPlacedEvent.cs.
        var json = $$"""
            {
              "eventId": "{{Guid.NewGuid()}}",
              "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
              "aggregateId": "{{orderId}}",
              "version": 1,
              "customerId": "{{Guid.NewGuid()}}",
              "totalAmount": 129.99
            }
            """;

        producer.Produce(OrderPlacedTopic, new Message<Null, string> { Value = json });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private string ConsumePaymentProcessed(TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(PaymentProcessedTopic);
        var result = consumer.Consume(timeout);
        Assert.NotNull(result);
        return result.Message.Value;
    }

    private static async Task<PaymentEntity> PollForPaymentAsync(IServiceProvider provider, Guid orderId, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            await using var scope = provider.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
            var payment = await dbContext.Payments.SingleOrDefaultAsync(p => p.OrderId == orderId);
            if (payment is not null)
            {
                return payment;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        throw new TimeoutException($"No Payment row appeared for order {orderId} within {timeout}.");
    }
}
