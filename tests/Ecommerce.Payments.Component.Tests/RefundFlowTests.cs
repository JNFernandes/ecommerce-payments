using System.Text.Json;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.DeadLetters;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Full refund flow (US-04) with real Kafka and PostgreSQL containers: OrderPlaced → Processed,
/// OrderCancelled → Refunded → <c>PaymentRefunded</c> on <c>payments.payment-refunded</c>, and the
/// dead-letter fallback when that publish fails.
/// </summary>
public class RefundFlowTests : IAsyncLifetime
{
    private const string OrderPlacedTopic = "orders.order-placed";
    private const string OrderCancelledTopic = "orders.order-cancelled";
    private const string PaymentRefundedTopic = "payments.payment-refunded";
    private const string UnreachableBroker = "127.0.0.1:1";

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
    public async Task FullFlow_OrderPlaced_Then_OrderCancelled_Publishes_PaymentRefunded()
    {
        await using var provider = BuildServiceProvider(_kafka.GetBootstrapAddress());
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        var orderId = Guid.NewGuid();
        var cancellationEventId = Guid.NewGuid();

        foreach (var service in hostedServices)
        {
            await service.StartAsync(CancellationToken.None);
        }

        PaymentEntity refundedRow;
        try
        {
            Produce(OrderPlacedTopic, OrderPlacedJson(orderId));
            await PollForStatusAsync(orderId, PaymentStatus.Processed);

            Produce(OrderCancelledTopic, OrderCancelledJson(orderId, cancellationEventId));
            refundedRow = await PollForStatusAsync(orderId, PaymentStatus.Refunded);
        }
        finally
        {
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None);
            }
        }

        Assert.Equal(cancellationEventId, refundedRow.RefundSourceEventId);

        var message = KafkaTestConsumer.SubscribeAndConsume<string>(
            _kafka.GetBootstrapAddress(), PaymentRefundedTopic, TimeSpan.FromSeconds(30));
        Assert.NotNull(message);
        Assert.Equal(refundedRow.Id.ToString(), message.Message.Key);

        var root = JsonDocument.Parse(message.Message.Value).RootElement;
        Assert.NotEqual(Guid.Empty, root.GetProperty("eventId").GetGuid());
        Assert.Equal(refundedRow.Id, root.GetProperty("aggregateId").GetGuid());
        Assert.Equal(orderId, root.GetProperty("orderId").GetGuid());
        Assert.Equal(129.99m, root.GetProperty("amount").GetDecimal());
        Assert.Equal("USD", root.GetProperty("currency").GetString());
        Assert.Equal(1, root.GetProperty("version").GetInt32());
        Assert.Equal(root.GetProperty("refundedAt").GetDateTimeOffset(), root.GetProperty("occurredAt").GetDateTimeOffset());
        Assert.Equal(refundedRow.RefundedAt!.Value.ToUnixTimeMilliseconds(), root.GetProperty("refundedAt").GetDateTimeOffset().ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task PublishFailure_DeadLettersRefund_And_RetryWorker_Delivers_It()
    {
        var orderId = Guid.NewGuid();

        // Broker unreachable: OrderPlaced and OrderCancelled are handled directly; both publishes
        // fail and are dead-lettered after the successful saves.
        await using (var brokerDown = BuildServiceProvider(UnreachableBroker))
        {
            using var placed = CreateConsumer<OrderPlacedConsumer>(brokerDown);
            using var cancelled = CreateConsumer<OrderCancelledConsumer>(brokerDown);
            Assert.True(await placed.HandleMessageAsync(OrderPlacedJson(orderId), CancellationToken.None));
            Assert.True(await cancelled.HandleMessageAsync(OrderCancelledJson(orderId, Guid.NewGuid()), CancellationToken.None));
        }

        var payment = await LoadPaymentAsync(orderId);
        Assert.Equal(PaymentStatus.Refunded, payment.Status);

        PaymentDeadLetterEntity refundDeadLetter;
        await using (var dbContext = CreateDbContext())
        {
            refundDeadLetter = await dbContext.PaymentDeadLetters.AsNoTracking()
                .SingleAsync(d => d.PaymentId == payment.Id && d.EventType == "PaymentRefundedEvent");
        }

        Assert.Equal(PaymentRefundedTopic, refundDeadLetter.Topic);
        Assert.Equal(payment.Id.ToString(), refundDeadLetter.MessageKey);
        Assert.Equal(DeadLetterStatus.PendingRetry, refundDeadLetter.Status);

        // Broker back: the US-03 retry processor delivers both dead letters unchanged.
        await using (var brokerUp = BuildServiceProvider(_kafka.GetBootstrapAddress()))
        {
            await using var scope = brokerUp.CreateAsyncScope();
            Assert.Equal(2, await scope.ServiceProvider.GetRequiredService<IDeadLetterRetryProcessor>()
                .RunCycleAsync(CancellationToken.None));
        }

        var message = KafkaTestConsumer.SubscribeAndConsume<string>(
            _kafka.GetBootstrapAddress(), PaymentRefundedTopic, TimeSpan.FromSeconds(30));
        Assert.NotNull(message);
        Assert.Equal(payment.Id.ToString(), message.Message.Key);
        Assert.Equal(EventIdOf(refundDeadLetter.Payload), EventIdOf(message.Message.Value));

        await using (var dbContext = CreateDbContext())
        {
            var resolved = await dbContext.PaymentDeadLetters.AsNoTracking().SingleAsync(d => d.Id == refundDeadLetter.Id);
            Assert.Equal(DeadLetterStatus.Resolved, resolved.Status);
        }
    }

    [Fact]
    public async Task Redelivered_OrderCancelled_Produces_Exactly_One_PaymentRefunded()
    {
        await using var provider = BuildServiceProvider(_kafka.GetBootstrapAddress());
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        var orderId = Guid.NewGuid();
        var cancellation = OrderCancelledJson(orderId, Guid.NewGuid());

        foreach (var service in hostedServices)
        {
            await service.StartAsync(CancellationToken.None);
        }

        PaymentEntity refunded;
        try
        {
            Produce(OrderPlacedTopic, OrderPlacedJson(orderId));
            await PollForStatusAsync(orderId, PaymentStatus.Processed);

            // The exact same message twice, as Kafka would redeliver it.
            Produce(OrderCancelledTopic, cancellation);
            Produce(OrderCancelledTopic, cancellation);
            refunded = await PollForStatusAsync(orderId, PaymentStatus.Refunded);
            await Task.Delay(TimeSpan.FromSeconds(3)); // let the second copy be handled
        }
        finally
        {
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None);
            }
        }

        var keys = ConsumeAllKeys(PaymentRefundedTopic, idle: TimeSpan.FromSeconds(5));
        Assert.Single(keys, key => key == refunded.Id.ToString());
    }

    private List<string> ConsumeAllKeys(string topic, TimeSpan idle)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Subscribe(topic);

        var keys = new List<string>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        var lastMessageAt = DateTime.UtcNow;
        while (DateTime.UtcNow < deadline && (keys.Count == 0 || DateTime.UtcNow - lastMessageAt < idle))
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is not null)
                {
                    keys.Add(result.Message.Key);
                    lastMessageAt = DateTime.UtcNow;
                }
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                // Topic not visible yet.
            }
        }

        return keys;
    }

    internal ServiceProvider BuildServiceProvider(string bootstrapServers)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddScoped<IPaymentDeadLetterRepository, PaymentDeadLetterRepository>();
        services.AddSingleton<PaymentEventPublisher>();
        services.AddSingleton<IPaymentEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
        services.AddSingleton<IRawEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<RefundPaymentHandler>();
        services.AddScoped<IDeadLetterRetryProcessor, DeadLetterRetryProcessor>();
        services.AddOptions<DeadLetterRetryOptions>();
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = bootstrapServers;
                o.ConsumerGroupId = $"component-tests-{Guid.NewGuid()}";
                o.OrderPlacedTopic = OrderPlacedTopic;
                o.OrderCancelledTopic = OrderCancelledTopic;
                o.PaymentProcessedTopic = "payments.payment-processed";
                o.PaymentFailedTopic = "payments.payment-failed";
                o.PaymentRefundedTopic = PaymentRefundedTopic;
            });
        services.AddSingleton<IHostedService, OrderPlacedConsumer>();
        services.AddSingleton<IHostedService, OrderCancelledConsumer>();

        return services.BuildServiceProvider();
    }

    internal static TConsumer CreateConsumer<TConsumer>(IServiceProvider provider)
        where TConsumer : KafkaConsumerBase =>
        (TConsumer)Activator.CreateInstance(
            typeof(TConsumer),
            provider.GetRequiredService<IOptions<KafkaOptions>>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<TConsumer>>())!;

    internal void Produce(string topic, string json)
    {
        using var producer = new ProducerBuilder<Null, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        producer.Produce(topic, new Message<Null, string> { Value = json });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    internal async Task<PaymentEntity> PollForStatusAsync(Guid orderId, PaymentStatus status)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            await using var dbContext = CreateDbContext();
            var payment = await dbContext.Payments.AsNoTracking().SingleOrDefaultAsync(p => p.OrderId == orderId);
            if (payment?.Status == status)
            {
                return payment;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"Payment for order {orderId} did not reach {status}.");
    }

    private async Task<PaymentEntity> LoadPaymentAsync(Guid orderId)
    {
        await using var dbContext = CreateDbContext();
        return await dbContext.Payments.AsNoTracking().SingleAsync(p => p.OrderId == orderId);
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private static Guid EventIdOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("eventId").GetGuid();

    internal static string OrderPlacedJson(Guid orderId) => $$"""
        {
          "eventId": "{{Guid.NewGuid()}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "totalAmount": 129.99
        }
        """;

    internal static string OrderCancelledJson(Guid orderId, Guid eventId) => $$"""
        {
          "eventId": "{{eventId}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "reason": "Customer cancelled"
        }
        """;
}
