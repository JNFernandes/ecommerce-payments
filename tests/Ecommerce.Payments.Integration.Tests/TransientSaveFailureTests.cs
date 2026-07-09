using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Simulates a real, temporary PostgreSQL outage (via Docker container pause/unpause, which
/// freezes the container almost instantly) rather than a mocked exception, so this genuinely
/// exercises <c>ProcessPaymentHandler</c>'s retry against real infrastructure per
/// quickstart.md Scenario 3.
/// </summary>
public class TransientSaveFailureTests : IAsyncLifetime
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
    public async Task HandleMessageAsync_TransientPostgresOutage_RetriesAndEventuallySucceeds()
    {
        // A short connection timeout keeps the "outage" attempt fast, so the retry has time
        // to succeed once the container is unpaused, without this test taking Npgsql's much
        // longer default connection timeout.
        var connectionString = _postgres.GetConnectionString() + ";Timeout=3;Command Timeout=3";
        var provider = BuildServiceProvider(connectionString);
        using var consumer = CreateConsumer(provider);
        var orderId = Guid.NewGuid();

        // Pause for longer than the 3s connection Timeout so the first save attempt genuinely
        // fails (proving a retry happens), then unpause so a subsequent retry can succeed.
        await _postgres.PauseAsync();
        var unpauseTask = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(4));
            await _postgres.UnpauseAsync();
        });

        try
        {
            var committed = await consumer.HandleMessageAsync(BuildOrderPlacedJson(orderId), CancellationToken.None);

            Assert.True(committed);

            await using var dbContext = provider.GetRequiredService<PaymentsDbContext>();
            var count = await dbContext.Payments.CountAsync(p => p.OrderId == orderId);
            Assert.Equal(1, count);

            var publisher = (FakePaymentEventPublisher)provider.GetRequiredService<IPaymentEventPublisher>();
            Assert.Single(publisher.Published, p => p.OrderId == orderId);
        }
        finally
        {
            await unpauseTask;
        }
    }

    private static ServiceProvider BuildServiceProvider(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(connectionString));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddSingleton<IPaymentEventPublisher, FakePaymentEventPublisher>();
        services.AddScoped<ProcessPaymentHandler>();
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = "localhost:9092";
                o.ConsumerGroupId = "integration-tests";
                o.OrderPlacedTopic = "orders.order-placed";
                o.PaymentProcessedTopic = "payments.payment-processed";
            });

        return services.BuildServiceProvider();
    }

    private static OrderPlacedConsumer CreateConsumer(ServiceProvider provider) => new(
        provider.GetRequiredService<IOptions<KafkaOptions>>(),
        provider.GetRequiredService<IServiceScopeFactory>(),
        provider.GetRequiredService<ILogger<OrderPlacedConsumer>>());

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
