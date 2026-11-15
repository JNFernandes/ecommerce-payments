using Confluent.Kafka;
using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// US-07 User Story 3 (FR-007, SC-003) with real Kafka and PostgreSQL: a payment that is
/// processed and then refunded while the broker is down has its two events published in
/// recording order once the broker is back — the refund is never published before the
/// processed event, even though both are due.
/// </summary>
public class OutboxOrderingTests : IAsyncLifetime
{
    private const string PaymentProcessedTopic = "payments.payment-processed";
    private const string PaymentRefundedTopic = "payments.payment-refunded";

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
    public async Task Processed_Is_Published_Before_Refunded_After_Outage()
    {
        var orderId = Guid.NewGuid();

        await using (var brokerDown = BuildServiceProvider("127.0.0.1:1"))
        {
            using var placed = RefundFlowTests.CreateConsumer<OrderPlacedConsumer>(brokerDown);
            using var cancelled = RefundFlowTests.CreateConsumer<OrderCancelledConsumer>(brokerDown);
            Assert.True(await placed.HandleMessageAsync(RefundFlowTests.OrderPlacedJson(orderId), CancellationToken.None));
            Assert.True(await cancelled.HandleMessageAsync(RefundFlowTests.OrderCancelledJson(orderId, Guid.NewGuid()), CancellationToken.None));

            // Only the processed entry is attempted: the refund waits behind it.
            Assert.Equal(1, await brokerDown.RunRelayCycleAsync());
        }

        var entries = await EntriesAsync();
        Assert.Equal(["PaymentProcessedEvent", "PaymentRefundedEvent"], entries.Select(e => e.EventType).ToArray());
        Assert.Equal(1, entries[0].RetryCount);
        Assert.Equal(0, entries[1].RetryCount);
        Assert.All(entries, e => Assert.Equal(OutboxStatus.Pending, e.Status));

        await using (var dbContext = CreateDbContext())
        {
            await dbContext.Database.ExecuteSqlAsync($"""UPDATE payment_outbox SET "NextAttemptAt" = {DateTimeOffset.UtcNow.AddSeconds(-1)}""");
        }

        await using (var brokerUp = BuildServiceProvider(_kafka.GetBootstrapAddress()))
        {
            Assert.Equal(2, await brokerUp.RunRelayCycleAsync());
        }

        entries = await EntriesAsync();
        Assert.All(entries, e => Assert.Equal(OutboxStatus.Published, e.Status));
        Assert.True(entries[0].PublishedAt < entries[1].PublishedAt, "Processed must be published before refunded.");

        var processedAt = BrokerTimestamp(PaymentProcessedTopic);
        var refundedAt = BrokerTimestamp(PaymentRefundedTopic);
        Assert.True(processedAt <= refundedAt, $"processed {processedAt:O} vs refunded {refundedAt:O}");
    }

    private async Task<List<OutboxEntryEntity>> EntriesAsync()
    {
        await using var dbContext = CreateDbContext();
        return await dbContext.PaymentOutbox.AsNoTracking().OrderBy(e => e.Sequence).ToListAsync();
    }

    private DateTime BrokerTimestamp(string topic)
    {
        var message = KafkaTestConsumer.SubscribeAndConsume(_kafka.GetBootstrapAddress(), topic, TimeSpan.FromSeconds(30));
        Assert.NotNull(message);
        return message.Message.Timestamp.UtcDateTime;
    }

    private ServiceProvider BuildServiceProvider(string bootstrapServers)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        services.AddScoped<ProcessPaymentHandler>();
        services.AddScoped<RefundPaymentHandler>();
        services.AddOptions<PaymentPolicyOptions>().Configure(o => o.MaxAmountThreshold = 10_000m);
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = bootstrapServers;
                o.ConsumerGroupId = $"component-tests-{Guid.NewGuid()}";
                o.PaymentProcessedTopic = PaymentProcessedTopic;
                o.PaymentRefundedTopic = PaymentRefundedTopic;
            });
        services.AddOutboxRelay(hostWorker: false);
        return services.BuildServiceProvider();
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
