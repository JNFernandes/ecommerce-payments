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

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Verifies the edge case from spec.md Edge Cases / Constitution Principle II: a publish
/// failure occurring *after* a successful save must not be lost or roll back the payment — it
/// is dead-lettered, and the Consumer still commits the offset.
/// </summary>
public class PublishFailureDeadLetterTests : IAsyncLifetime
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
    public async Task PublishFailureAfterSuccessfulSave_WritesDeadLetterAndStillCommitsOffset()
    {
        await using var provider = BuildServiceProvider();
        using var consumer = new OrderPlacedConsumer(
            provider.GetRequiredService<IOptions<KafkaOptions>>(),
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<ILogger<OrderPlacedConsumer>>());

        var orderId = Guid.NewGuid();

        var committed = await consumer.HandleMessageAsync(BuildOrderPlacedJson(orderId), CancellationToken.None);

        Assert.True(committed, "The offset must still be committed — the Payment row is already correct.");

        await using var dbContext = provider.GetRequiredService<PaymentsDbContext>();
        var payment = await dbContext.Payments.SingleAsync(p => p.OrderId == orderId);
        Assert.Equal(PaymentStatus.Processed, payment.Status);

        var deadLetter = await dbContext.PaymentDeadLetters.SingleAsync(d => d.PaymentId == payment.Id);
        Assert.Equal(nameof(Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents.PaymentProcessedEvent), deadLetter.EventType);
        Assert.Contains(orderId.ToString(), deadLetter.Payload, StringComparison.Ordinal);
        Assert.False(string.IsNullOrWhiteSpace(deadLetter.FailureReason));
    }

    private ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(_postgres.GetConnectionString()));
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddScoped<ProcessPaymentHandler>();

        // The Consumer's own Kafka config is irrelevant here (HandleMessageAsync is driven
        // directly, no real broker is subscribed to), but the Publisher is deliberately pointed
        // at an unreachable broker so its real publish path fails fast (MessageTimeoutMs=10s)
        // and exercises the genuine dead-letter fallback in PaymentEventPublisher.
        services
            .AddOptions<KafkaOptions>()
            .Configure(o =>
            {
                o.BootstrapServers = "127.0.0.1:1";
                o.ConsumerGroupId = "component-tests-deadletter";
                o.OrderPlacedTopic = "orders.order-placed";
                o.PaymentProcessedTopic = "payments.payment-processed";
            });
        services.AddSingleton<IPaymentEventPublisher, PaymentEventPublisher>();

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
