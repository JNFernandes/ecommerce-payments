using Ecommerce.Payments.Consumer.Consumers;
using Ecommerce.Payments.Infrastructure.DeadLetters;
using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName));

builder.Services
    .AddOptions<PaymentPolicyOptions>()
    .Bind(builder.Configuration.GetSection(PaymentPolicyOptions.SectionName));

builder.Services
    .AddOptions<DeadLetterRetryOptions>()
    .Bind(builder.Configuration.GetSection(DeadLetterRetryOptions.SectionName))
    .Validate(
        options => options.IsValid(),
        "DeadLetterRetry settings must be positive and MaxDelayMs must be >= BaseDelayMs.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);

builder.Services.AddDbContext<PaymentsDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Payments")));

builder.Services.AddScoped<IPaymentRepository, PaymentRepository>();
builder.Services.AddScoped<IPaymentDeadLetterRepository, PaymentDeadLetterRepository>();
// Singleton: the Kafka producer it wraps is designed to be long-lived and reused, not
// recreated per message. One instance serves both the normal (dead-lettering) publish path and
// the raw publish used by the dead-letter retry worker.
builder.Services.AddSingleton<PaymentEventPublisher>();
builder.Services.AddSingleton<IPaymentEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
builder.Services.AddSingleton<IRawEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
builder.Services.AddScoped<ProcessPaymentHandler>();

builder.Services.AddScoped<IDeadLetterRetryProcessor, DeadLetterRetryProcessor>();

builder.Services.AddHostedService<OrderPlacedConsumer>();
builder.Services.AddHostedService<DeadLetterRetryWorker>();

var host = builder.Build();
host.Run();
