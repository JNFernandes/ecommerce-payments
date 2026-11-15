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
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// US-06 end to end with real Kafka and PostgreSQL, both real consumers and the real
/// <see cref="ConsumedDeadLetterStore"/>: malformed messages are stored once in
/// <c>consumed_dead_letters</c> and do not block their partition (US2), and a stored message
/// replayed by the documented procedure is processed exactly once (US3).
/// </summary>
public class MalformedMessageDeadLetterFlowTests : IAsyncLifetime
{
    private const string OrderPlacedTopic = "orders.order-placed";
    private const string OrderCancelledTopic = "orders.order-cancelled";

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
    public async Task Malformed_Messages_Are_Stored_Once_And_Do_Not_Block_The_Partition()
    {
        var orderId = Guid.NewGuid();

        await RunHostedAsync(async () =>
        {
            await ProduceAsync(OrderPlacedTopic, "order-1", "not json");
            await ProduceAsync(OrderCancelledTopic, null, """{"eventId":"6f1d2c3b-1111-4000-8000-000000000001","version":1}""");
            await PollAsync(async db => await db.ConsumedDeadLetters.CountAsync() == 2, "two dead letters");

            await ProduceAsync(OrderPlacedTopic, orderId.ToString(), RefundFlowTests.OrderPlacedJson(orderId));
            await PollAsync(
                async db => await db.Payments.AnyAsync(p => p.OrderId == orderId && p.Status == PaymentStatus.Processed),
                "the valid OrderPlaced to be processed");
        });

        await using var dbContext = CreateDbContext();
        var rows = await dbContext.ConsumedDeadLetters.AsNoTracking().OrderBy(d => d.Topic).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1, row.Attempts));

        var cancelled = rows.Single(r => r.Topic == OrderCancelledTopic);
        Assert.Null(cancelled.MessageKey);
        Assert.Contains("aggregateId", cancelled.Error, StringComparison.OrdinalIgnoreCase);

        var placed = rows.Single(r => r.Topic == OrderPlacedTopic);
        Assert.Equal("order-1", placed.MessageKey);
        Assert.Equal("not json", placed.Payload);
        Assert.Equal(0, placed.Offset);
        Assert.Contains(nameof(InvalidMessageException), placed.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Replayed_Dead_Letter_Is_Processed_Like_A_New_Message()
    {
        var orderId = Guid.NewGuid();
        var payload = RefundFlowTests.OrderPlacedJson(orderId).ReplaceLineEndings(string.Empty);

        // As if stored during an outage: a valid OrderPlaced that kept failing.
        await using (var dbContext = CreateDbContext())
        {
            dbContext.ConsumedDeadLetters.Add(new ConsumedDeadLetterEntity
            {
                Id = Guid.NewGuid(),
                Topic = OrderPlacedTopic,
                Partition = 0,
                Offset = 99,
                MessageKey = orderId.ToString(),
                Payload = payload,
                Error = "Npgsql.NpgsqlException: connection refused",
                Attempts = 5,
                FailedAt = DateTimeOffset.UtcNow
            });
            await dbContext.SaveChangesAsync();
        }

        await RunHostedAsync(async () =>
        {
            // The documented replay: produce Payload with MessageKey to Topic — twice.
            ConsumedDeadLetterEntity row;
            await using (var dbContext = CreateDbContext())
            {
                row = await dbContext.ConsumedDeadLetters.AsNoTracking().SingleAsync();
            }

            await ProduceAsync(row.Topic, row.MessageKey, row.Payload!);
            await PollAsync(async db => await db.Payments.AnyAsync(p => p.OrderId == orderId), "the replayed payment");

            var second = await ProduceAsync(row.Topic, row.MessageKey, row.Payload!);
            await PollUntilCommittedPastAsync(second.TopicPartitionOffset);
        });

        await using var check = CreateDbContext();
        var payment = await check.Payments.AsNoTracking().SingleAsync(p => p.OrderId == orderId);
        Assert.Equal(PaymentStatus.Processed, payment.Status);
    }

    private readonly string _groupId = $"component-tests-{Guid.NewGuid()}";

    private async Task RunHostedAsync(Func<Task> scenario)
    {
        await using var provider = BuildServiceProvider();
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

    private async Task PollAsync(Func<PaymentsDbContext, Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            await using var dbContext = CreateDbContext();
            if (await condition(dbContext))
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"Timed out waiting for {what}.");
    }

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

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddOrderPersistence();
        services.AddConsumedDeadLetterStore();
        services.AddScoped<IPaymentDeadLetterRepository, PaymentDeadLetterRepository>();
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
                o.PaymentProcessedTopic = "payments.payment-processed";
                o.PaymentFailedTopic = "payments.payment-failed";
                o.PaymentRefundedTopic = "payments.payment-refunded";
            });
        services.AddSingleton<IHostedService, OrderPlacedConsumer>();
        services.AddSingleton<IHostedService, OrderCancelledConsumer>();

        return services.BuildServiceProvider();
    }

    private async Task<DeliveryResult<string?, string>> ProduceAsync(string topic, string? key, string value)
    {
        using var producer = new ProducerBuilder<string?, string>(
            new ProducerConfig { BootstrapServers = _kafka.GetBootstrapAddress() }).Build();
        return await producer.ProduceAsync(topic, new Message<string?, string> { Key = key, Value = value });
    }

    private PaymentsDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(_postgres.GetConnectionString()).Options);
}
