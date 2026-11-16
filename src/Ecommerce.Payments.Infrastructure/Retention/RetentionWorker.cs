using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ecommerce.Payments.Infrastructure.Retention;

/// <summary>
/// Hosted service that applies the data-retention policy (US-09, ADR 0007; Constitution v1.4.0,
/// Principle VI "Data retention"). It runs one <see cref="IRetentionPurger"/> purge shortly after
/// startup and then every <see cref="RetentionOptions.IntervalMs"/>, each in a fresh DI scope,
/// and logs how many rows it deleted per store — at Information when anything was deleted, at
/// Debug otherwise.
///
/// Startup never waits on the database: <see cref="ExecuteAsync"/> yields before any work. A
/// failed run is logged and retried on the next interval instead of crashing the host; payment
/// processing is unaffected. On shutdown the stopping token cancels the run in flight (its
/// current batch rolls back; earlier batches stay committed) and the loop exits.
/// </summary>
public sealed class RetentionWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RetentionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RetentionWorker> _logger;

    /// <summary>Creates the worker.</summary>
    public RetentionWorker(
        IServiceScopeFactory scopeFactory,
        IOptions<RetentionOptions> options,
        TimeProvider timeProvider,
        ILogger<RetentionWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Return control to the host before any I/O so startup never depends on PostgreSQL.
        await Task.Yield();

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_options.IntervalMs), _timeProvider);

        do
        {
            try
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                var purger = scope.ServiceProvider.GetRequiredService<IRetentionPurger>();
                var result = await purger.PurgeAsync(stoppingToken);
                Log(result);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retention purge failed; retrying on the next interval.");
            }
        }
        while (await WaitForNextTickAsync(timer, stoppingToken));
    }

    private void Log(PurgeResult result)
    {
        var level = result.Total > 0 ? LogLevel.Information : LogLevel.Debug;
        _logger.Log(
            level,
            "Retention purge deleted {OutboxEntries} outbox entries, {DeadLetters} dead letters, {Markers} cancellation markers.",
            result.OutboxEntries,
            result.DeadLetters,
            result.Markers);
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
