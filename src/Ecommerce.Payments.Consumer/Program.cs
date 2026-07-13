using Ecommerce.Payments.Consumer.Consumers;
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

builder.Services.AddDbContext<PaymentsDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Payments")));

builder.Services.AddScoped<IPaymentRepository, PaymentRepository>();
// Singleton: the Kafka producer it wraps is designed to be long-lived and reused, not
// recreated per message.
builder.Services.AddSingleton<IPaymentEventPublisher, PaymentEventPublisher>();
builder.Services.AddScoped<ProcessPaymentHandler>();

builder.Services.AddHostedService<OrderPlacedConsumer>();

var host = builder.Build();
host.Run();
