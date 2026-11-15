using System.Text.Json;
using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// US-07 User Story 1 with real Kafka and PostgreSQL: an outcome recorded while nothing could
/// publish it — the process "stopped" before its relay ran, or the broker was down — is
/// published exactly once afterwards, with the <c>eventId</c> it was recorded with.
/// </summary>
public class OutboxCrashRecoveryTests : IAsyncLifetime
{
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
    public async Task Outcome_Saved_While_Relay_Is_Down_Is_Published_Once_After_Restart()
    {
        var orderId = Guid.NewGuid();
        OutboxEntryEntity pending;

        // "Process" 1: consumers only — the outcome is saved, nothing is published, then it stops.
        await using (var withoutRelay = BuildServiceProvider(_kafka.GetBootstrapAddress(), hostRelay: false))
        {
            await RunHostedAsync(withoutRelay, async () =>
            {
                Produce(OrderPlacedTopic, RefundFlowTests.OrderPlacedJson(orderId));
                pending = await PollForEntryAsync(orderId, OutboxStatus.Pending);
            });
        }

        pending = await PollForEntryAsync(orderId, OutboxStatus.Pending);
        Assert.Empty(ConsumeAll(PaymentProcessedTopic, TimeSpan.FromSeconds(3)));

        // "Process" 2: the relay starts and publishes the recorded entry.
        await using (var withRelay = BuildServiceProvider(_kafka.GetBootstrapAddress(), hostRelay: true))
        {
            await RunHostedAsync(withRelay, async () =>
                await PollForEntryAsync(orderId, OutboxStatus.Published));
        }

        var messages = ConsumeAll(PaymentProcessedTopic, TimeSpan.FromSeconds(5));
        var message = Assert.Single(messages);
        Assert.Equal(pending.EventId, EventIdOf(message.Value));
        Assert.Equal(pending.MessageKey, message.Key);

        var published = await PollForEntryAsync(orderId, OutboxStatus.Published);
        Assert.NotNull(published.PublishedAt);
    }

    [Fact]
    public async Task Broker_Outage_Keeps_Entry_Pending_And_Delivers_It_Once_After_Recovery()
    {
        var orderId = Guid.NewGuid();

        await using (var brokerDown = BuildServiceProvider("127.0.0.1:1", hostRelay: false))
        {
            using var consumer = RefundFlowTests.CreateConsumer<OrderPlacedConsumer>(brokerDown);
            Assert.True(await consumer.HandleMessageAsync(RefundFlowTests.OrderPlacedJson(orderId), CancellationToken.None));
            Assert.Equal(1, await brokerDown.RunRelayCycleAsync());
        }

        var failed = await PollForEntryAsync(orderId, OutboxStatus.Pending);
        Assert.Equal(1, failed.RetryCount);
        Assert.False(string.IsNullOrWhiteSpace(failed.LastError));

        // Broker back; make the entry due now instead of waiting out the backoff.
        await using (var dbContext = CreateDbContext())
        {
            await dbContext.Database.ExecuteSqlAsync(
                $"""UPDATE payment_outbox SET "NextAttemptAt" = now() WHERE "Id" = {failed.Id}""");
        }

        await using (var brokerUp = BuildServiceProvider(_kafka.GetBootstrapAddress(), hostRelay: false))
        {
            Assert.Equal(1, await brokerUp.RunRelayCycleAsync());
            Assert.Equal(0, await brokerUp.RunRelayCycleAsync());
        }

        var message = Assert.Single(ConsumeAll(PaymentProcessedTopic, TimeSpan.FromSeconds(5)));
        Assert.Equal(failed.EventId, EventIdOf(message.Value));
        Assert.Equal(OutboxStatus.Published, (await PollForEntryAsync(orderId, OutboxStatus.Published)).Status);
    }

    private static async Task RunHostedAsync(ServiceProvider provider, Func<Task> scenario)
    {
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hostedServices)
        {
            await service.StartAsync(CancellationToken.None);
        }

        try
        {
            await scenario();
        }
        finally
        {
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None);
            }
        }
    }

    private async Task<OutboxEntryEntity> PollForEntryAsync(Guid orderId, OutboxStatus status)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            await using var dbContext = CreateDbContext();
            var paymentId = await dbContext.Payments.Where(p => p.OrderId == orderId).Select(p => (Guid?)p.Id).SingleOrDefaultAsync();
            if (paymentId is not null)
            {
                var entry = await dbContext.PaymentOutbox.AsNoTracking().SingleOrDefaultAsync(e => e.PaymentId == paymentId);
                if (entry?.Status == status)
                {
                    return entry;
                }
            }

            await Task.Delay(200);
        }

        throw new TimeoutException($"No {status} outbox entry for order {orderId}.");
    }

    private List<(string Key, string Value)> ConsumeAll(string topic, TimeSpan idle)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            GroupId = $"component-tests-assert-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false
        }).Build();
        consumer.Subscribe(topic);

        var messages = new List<(string Key, string Value)>();
        var lastMessageAt = DateTime.UtcNow;
        while (DateTime.UtcNow - lastMessageAt < idle)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result is not null)
                {
                    messages.Add((result.Message.Key, result.Message.Value));
                    lastMessageAt = DateTime.UtcNow;
                }
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                // Topic not created yet: nothing published to it.
            }
        }

        return messages;
    }

    private ServiceProvider BuildServiceProvider(string bootstrapServers, bool hostRelay)
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
                o.BootstrapServers = bootstrapServers;
                o.ConsumerGroupId = $"component-tests-{Guid.NewGuid()}";
                o.OrderPlacedTopic = OrderPlacedTopic;
                o.PaymentProcessedTopic = PaymentProcessedTopic;
            });
        services.AddOutboxRelay(hostWorker: hostRelay);
        services.AddSingleton<IHostedService, OrderPlacedConsumer>();
        return services.BuildServiceProvider();
    }

    private void Produce(string topic, string json)
    {
        using var producer = new ProducerBuilder<Null, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        producer.Produce(topic, new Message<Null, string> { Value = json });
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private static Guid EventIdOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("eventId").GetGuid();
}
