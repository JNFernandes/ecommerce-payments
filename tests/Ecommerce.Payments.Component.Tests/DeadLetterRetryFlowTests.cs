using System.Text.Json;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.DeadLetters;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// End-to-end dead-letter retry (US-03): the first publish genuinely fails (unreachable broker),
/// the event is dead-lettered, and the retry processor later republishes it to a real Kafka
/// broker with the same payload, key and <c>eventId</c>.
/// </summary>
public class DeadLetterRetryFlowTests : IAsyncLifetime
{
    private const string PaymentProcessedTopic = "payments.payment-processed";
    private const string UnreachableBroker = "127.0.0.1:1";

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
    public async Task Should_Republish_DeadLettered_Event_With_Same_EventId_And_Key_And_Resolve_Row()
    {
        var orderId = Guid.NewGuid();
        var deadLetter = await DeadLetterViaFailedPublishAsync(orderId);

        await using (var brokerUp = BuildServiceProvider(_kafka.GetBootstrapAddress(), TimeProvider.System))
        {
            await using var scope = brokerUp.CreateAsyncScope();
            var processor = scope.ServiceProvider.GetRequiredService<IDeadLetterRetryProcessor>();

            var attempted = await processor.RunCycleAsync(CancellationToken.None);

            Assert.Equal(1, attempted);
        }

        var published = KafkaTestConsumer.SubscribeAndConsume<string>(
            _kafka.GetBootstrapAddress(), PaymentProcessedTopic, TimeSpan.FromSeconds(30));
        Assert.NotNull(published);
        Assert.Equal(deadLetter.MessageKey, published.Message.Key);
        Assert.Equal(EventIdOf(deadLetter.Payload), EventIdOf(published.Message.Value));
        Assert.Equal(orderId, JsonDocument.Parse(published.Message.Value).RootElement.GetProperty("orderId").GetGuid());

        var row = await LoadAsync(deadLetter.Id);
        Assert.Equal(DeadLetterStatus.Resolved, row.Status);
        Assert.NotNull(row.ResolvedAt);
        Assert.Equal(0, row.RetryCount);
        Assert.Equal(deadLetter.Payload, row.Payload);
    }

    /// <summary>
    /// Runs the real Consumer → Service → Repository → Publisher flow with the publisher pointed at
    /// an unreachable broker, so the publish fails for real and the event is dead-lettered.
    /// </summary>
    private async Task<PaymentDeadLetterEntity> DeadLetterViaFailedPublishAsync(Guid orderId)
    {
        await using var brokerDown = BuildServiceProvider(UnreachableBroker, TimeProvider.System);
        using var consumer = new OrderPlacedConsumer(
            brokerDown.GetRequiredService<IOptions<KafkaOptions>>(),
            brokerDown.GetRequiredService<IServiceScopeFactory>(),
            brokerDown.GetRequiredService<ILogger<OrderPlacedConsumer>>());

        Assert.True(await consumer.HandleMessageAsync(BuildOrderPlacedJson(orderId), CancellationToken.None));

        await using var scope = brokerDown.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
        var paymentId = (await dbContext.Payments.SingleAsync(p => p.OrderId == orderId)).Id;
        return await dbContext.PaymentDeadLetters.AsNoTracking().SingleAsync(d => d.PaymentId == paymentId);
    }

    internal ServiceProvider BuildServiceProvider(
        string bootstrapServers,
        TimeProvider timeProvider,
        Func<IServiceProvider, IRawEventPublisher>? rawPublisher = null,
        Action<DeadLetterRetryOptions>? configureRetry = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(timeProvider);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddScoped<IPaymentDeadLetterRepository, PaymentDeadLetterRepository>();
        services.AddSingleton<PaymentEventPublisher>();
        services.AddSingleton<IPaymentEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
        services.AddSingleton(rawPublisher ?? (sp => sp.GetRequiredService<PaymentEventPublisher>()));
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<IDeadLetterRetryProcessor, DeadLetterRetryProcessor>();
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = bootstrapServers;
                o.ConsumerGroupId = $"component-tests-{Guid.NewGuid()}";
                o.OrderPlacedTopic = "orders.order-placed";
                o.PaymentProcessedTopic = PaymentProcessedTopic;
            });
        services.AddOptions<DeadLetterRetryOptions>().Configure(o => configureRetry?.Invoke(o));

        return services.BuildServiceProvider();
    }

    private async Task<PaymentDeadLetterEntity> LoadAsync(Guid id)
    {
        await using var dbContext = new PaymentsDbContext(
            new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
        return await dbContext.PaymentDeadLetters.AsNoTracking().SingleAsync(d => d.Id == id);
    }

    private static Guid EventIdOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("eventId").GetGuid();

    private static string BuildOrderPlacedJson(Guid orderId) => $$"""
        {
          "eventId": "{{Guid.NewGuid()}}",
          "occurredAt": "{{DateTimeOffset.UtcNow:O}}",
          "aggregateId": "{{orderId}}",
          "version": 1,
          "customerId": "{{Guid.NewGuid()}}",
          "totalAmount": 129.99
        }
        """;
}
