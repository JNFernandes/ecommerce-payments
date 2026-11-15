using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
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
        "DeadLetterRetry settings must be positive and MaxDelayMs must be >= BaseDelayMs.")
    .ValidateOnStart();

builder.Services
    .AddOptions<PersistenceOptions>()
    .Bind(builder.Configuration.GetSection(PersistenceOptions.SectionName))
    .Validate(options => options.OrderLockTimeoutMs > 0, "Persistence:OrderLockTimeoutMs must be positive.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);

builder.Services.AddDbContext<PaymentsDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Payments")));

builder.Services.AddScoped<IPaymentRepository, PaymentRepository>();
builder.Services.AddScoped<IPaymentOutboxRepository, PaymentOutboxRepository>();
// Per-order lock (ADR 0004) and cancellation markers (US-05).
builder.Services.AddOrderPersistence();
// Inbound messages that cannot be handled (US-06, ADR 0005).
builder.Services.AddConsumedDeadLetterStore();
// Singleton: the Kafka producer it wraps is designed to be long-lived and reused, not
// recreated per message. One instance serves both the normal (dead-lettering) publish path and
// the raw publish used by the dead-letter retry worker.
builder.Services.AddSingleton<PaymentEventPublisher>();
builder.Services.AddSingleton<IPaymentEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
builder.Services.AddSingleton<IRawEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
builder.Services.AddScoped<ProcessPaymentHandler>();
builder.Services.AddScoped<RefundPaymentHandler>();

builder.Services.AddScoped<IOutboxRelayProcessor, OutboxRelayProcessor>();

builder.Services.AddHostedService<OrderPlacedConsumer>();
builder.Services.AddHostedService<OrderCancelledConsumer>();
builder.Services.AddHostedService<OutboxRelayWorker>();

var host = builder.Build();
host.Run();
