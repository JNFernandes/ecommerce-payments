using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Constitution v1.3.0, Principle II with Kafka unreachable (US-07): the payment and its outbox
/// entry are saved and the Consumer commits the offset without any publish being attempted; the
/// relay's failed attempt is recorded on the same entry, which stays <c>Pending</c> and keeps
/// its <c>eventId</c> — nothing is lost and no second entry is created.
/// </summary>
public class OutboxPublishFailureTests : IAsyncLifetime
{
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
    public async Task Broker_Down_Saves_Payment_And_Pending_Entry_And_Relay_Records_The_Failure()
    {
        await using var provider = BuildServiceProvider();
        using var consumer = new OrderPlacedConsumer(
            provider.GetRequiredService<IOptions<KafkaOptions>>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<OrderPlacedConsumer>>());
        var orderId = Guid.NewGuid();

        var committed = await consumer.HandleMessageAsync(BuildOrderPlacedJson(orderId), CancellationToken.None);

        Assert.True(committed, "The offset is committed once the payment and its outbox entry are saved.");

        OutboxEntryEntity recorded;
        await using (var dbContext = CreateDbContext())
        {
            var payment = await dbContext.Payments.SingleAsync(p => p.OrderId == orderId);
            Assert.Equal(PaymentStatus.Processed, payment.Status);

            recorded = await dbContext.PaymentOutbox.AsNoTracking().SingleAsync(e => e.PaymentId == payment.Id);
            Assert.Equal(nameof(Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents.PaymentProcessedEvent), recorded.EventType);
            Assert.Contains(orderId.ToString(), recorded.Payload, StringComparison.Ordinal);
            Assert.Equal(OutboxStatus.Pending, recorded.Status);
            Assert.Equal(0, recorded.RetryCount);
            Assert.Null(recorded.LastError);
            Assert.Equal("payments.payment-processed", recorded.Topic);
            Assert.Equal(payment.Id.ToString(), recorded.MessageKey);
        }

        // One relay cycle against the unreachable broker (fails within MessageTimeoutMs).
        Assert.Equal(1, await provider.RunRelayCycleAsync());

        await using (var dbContext = CreateDbContext())
        {
            var entry = await dbContext.PaymentOutbox.AsNoTracking().SingleAsync();
            Assert.Equal(recorded.Id, entry.Id);
            Assert.Equal(recorded.EventId, entry.EventId);
            Assert.Equal(recorded.Payload, entry.Payload);
            Assert.Equal(OutboxStatus.Pending, entry.Status);
            Assert.Equal(1, entry.RetryCount);
            Assert.False(string.IsNullOrWhiteSpace(entry.LastError));
            Assert.Null(entry.PublishedAt);
        }
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        services.AddScoped<ProcessPaymentHandler>();
        // Unreachable broker: the relay's publish attempt fails (MessageTimeoutMs = 10 s).
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = "127.0.0.1:1";
                o.ConsumerGroupId = "component-tests-outbox-failure";
                o.OrderPlacedTopic = "orders.order-placed";
                o.PaymentProcessedTopic = "payments.payment-processed";
            });
        services.AddSingleton(TimeProvider.System);
        services.AddOutboxRelay(hostWorker: false);

        return services.BuildServiceProvider();
    }

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
