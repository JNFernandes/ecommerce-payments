using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Outbox;

/// <summary>
/// The outbox relay (US-07, ADR 0006; Constitution v1.3.0, Principle VI "Outbox relay"): the
/// only component that publishes payment events. It runs one <see cref="IOutboxRelayProcessor"/>
/// cycle immediately, then every <see cref="OutboxRelayOptions.PollIntervalMs"/>, each in a
/// fresh DI scope.
///
/// Startup never waits on Kafka or the database: <see cref="ExecuteAsync"/> yields before doing
/// any work, and failures inside a cycle are logged and retried on the next interval instead of
/// crashing the host (spec FR-012/FR-013, Constitution "Startup &amp; broker availability"). On
/// shutdown the stopping token cancels the cycle in flight and the loop exits.
/// </summary>
public sealed class OutboxRelayWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxRelayOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<OutboxRelayWorker> _logger;

    /// <summary>Creates the worker.</summary>
    public OutboxRelayWorker(
        IServiceScopeFactory scopeFactory,
        IOptions<OutboxRelayOptions> options,
        TimeProvider timeProvider,
        ILogger<OutboxRelayWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Return control to the host before any I/O so startup never depends on Kafka/PostgreSQL.
        await Task.Yield();

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_options.PollIntervalMs), _timeProvider);

        do
        {
            try
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                var processor = scope.ServiceProvider.GetRequiredService<IOutboxRelayProcessor>();
                await processor.RunCycleAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outbox relay cycle failed; retrying on the next interval.");
            }
        }
        while (await WaitForNextTickAsync(timer, stoppingToken));
    }

    private static async Task<bool> WaitForNextTickAsync(PeriodicTimer timer, CancellationToken stoppingToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
