using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Retention;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(
        options => options.ConsumerMaxAttempts > 0
            && options.ConsumerRetryBaseDelayMs > 0
            && options.ConsumerRetryMaxDelayMs >= options.ConsumerRetryBaseDelayMs,
        "Kafka consumer retry settings must be positive and ConsumerRetryMaxDelayMs >= ConsumerRetryBaseDelayMs.")
    .ValidateOnStart();

builder.Services
    .AddOptions<PaymentPolicyOptions>()
    .Bind(builder.Configuration.GetSection(PaymentPolicyOptions.SectionName));

builder.Services
    .AddOptions<OutboxRelayOptions>()
    .Bind(builder.Configuration.GetSection(OutboxRelayOptions.SectionName))
    .Validate(
        options => options.IsValid(),
        "OutboxRelay settings must be positive and MaxDelayMs must be >= BaseDelayMs.")
    .ValidateOnStart();

builder.Services
    .AddOptions<RetentionOptions>()
    .Bind(builder.Configuration.GetSection(RetentionOptions.SectionName))
    .Validate(
        options => options.IsValid(),
        "Retention settings: IntervalMs and BatchSize must be positive and retention days must be >= 0.")
    .ValidateOnStart();

builder.Services
    .AddOptions<PersistenceOptions>()
    .Bind(builder.Configuration.GetSection(PersistenceOptions.SectionName))
    .Validate(options => options.OrderLockTimeoutMs > 0, "Persistence:OrderLockTimeoutMs must be positive.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);

builder.Services
    .AddOptions<DatabaseMigrationOptions>()
    .Bind(builder.Configuration.GetSection(DatabaseMigrationOptions.SectionName))
    .Validate(options => options.IsValid(), "DatabaseMigration:MaxAttempts and RetryDelayMs must be positive.")
    .ValidateOnStart();

builder.Services.AddDbContext<PaymentsDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Payments")));

// Apply pending migrations before anything consumes or publishes. MUST stay the first hosted
// service: the host starts them in registration order and stops at the first failure.
builder.Services.AddDatabaseMigration();

builder.Services.AddScoped<IPaymentRepository, PaymentRepository>();
builder.Services.AddScoped<IPaymentOutboxRepository, PaymentOutboxRepository>();
// Per-order lock (ADR 0004) and cancellation markers (US-05).
builder.Services.AddOrderPersistence();
// Inbound messages that cannot be handled (US-06, ADR 0005).
builder.Services.AddConsumedDeadLetterStore();
// Transactional outbox (US-07, ADR 0006): handlers record events with the state change; only
// the relay publishes, through the raw publisher. Singleton: the Kafka producer it wraps is
// long-lived and reused.
builder.Services.AddPaymentOutbox();
builder.Services.AddSingleton<PaymentEventPublisher>();
builder.Services.AddSingleton<IRawEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
builder.Services.AddScoped<ProcessPaymentHandler>();
builder.Services.AddScoped<RefundPaymentHandler>();

builder.Services.AddScoped<IOutboxRelayProcessor, OutboxRelayProcessor>();

builder.Services.AddHostedService<OrderPlacedConsumer>();
builder.Services.AddHostedService<OrderCancelledConsumer>();
builder.Services.AddHostedService<OutboxRelayWorker>();
// Data retention (US-09, ADR 0007): hourly batched purge of old outbox entries, dead letters and markers.
builder.Services.AddRetention();
builder.Services.AddHostedService<RetentionWorker>();

var host = builder.Build();

try
{
    await host.RunAsync();
    return 0;
}
catch (DatabaseMigrationException)
{
    // Already logged as Critical by DatabaseMigrator; exit non-zero so the orchestrator sees it.
    return 1;
}
