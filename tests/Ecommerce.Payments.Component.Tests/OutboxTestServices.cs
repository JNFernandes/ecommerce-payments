using Ecommerce.Payments.Infrastructure.Messaging;
using Ecommerce.Payments.Infrastructure.Outbox;
using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Ecommerce.Payments.Component.Tests;

/// <summary>
/// Component-test wiring for the transactional outbox (US-07): the outbox writer used by the
/// handlers, the raw Kafka publisher, the relay processor and — unless a test drives cycles by
/// hand — the hosted <see cref="OutboxRelayWorker"/>. Uses <c>TryAdd</c> for the publisher, so a
/// test that registers its own <see cref="IRawEventPublisher"/> first keeps it.
/// </summary>
internal static class OutboxTestServices
{
    /// <summary>Fast poll so component tests see published events quickly.</summary>
    public const int TestPollIntervalMs = 100;

    public static IServiceCollection AddOutboxRelay(
        this IServiceCollection services,
        bool hostWorker = true,
        Action<OutboxRelayOptions>? configure = null)
    {
        services.AddPaymentOutbox();
        services.TryAddSingleton<PaymentEventPublisher>();
        services.TryAddSingleton<IRawEventPublisher>(sp => sp.GetRequiredService<PaymentEventPublisher>());
        services.TryAddScoped<IPaymentOutboxRepository, PaymentOutboxRepository>();
        services.TryAddScoped<IOutboxRelayProcessor, OutboxRelayProcessor>();
        services.AddOptions<OutboxRelayOptions>().Configure(o =>
        {
            o.PollIntervalMs = TestPollIntervalMs;
            configure?.Invoke(o);
        });

        if (hostWorker)
        {
            services.AddSingleton<IHostedService, OutboxRelayWorker>();
        }

        return services;
    }

    /// <summary>Runs one relay cycle in its own scope; returns the number of entries attempted.</summary>
    public static async Task<int> RunRelayCycleAsync(this IServiceProvider provider)
    {
        await using var scope = provider.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<IOutboxRelayProcessor>().RunCycleAsync(CancellationToken.None);
    }
}
