using Ecommerce.Payments.Infrastructure.Retention;
using Ecommerce.Payments.Infrastructure.Tests.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace Ecommerce.Payments.Infrastructure.Tests.Retention;

/// <summary><see cref="RetentionWorker"/> (US-09): timing, failure handling and logging.</summary>
public sealed class RetentionWorkerTests : IDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero));
    private readonly Mock<IRetentionPurger> _purger = new();
    private readonly RetentionOptions _options = new() { IntervalMs = 3_600_000 };
    private readonly ListLogger<RetentionWorker> _logger = new();
    private readonly ServiceProvider _services;
    private int _runs;

    public RetentionWorkerTests()
    {
        _services = new ServiceCollection()
            .AddScoped(_ => _purger.Object)
            .BuildServiceProvider();
    }

    public void Dispose() => _services.Dispose();

    [Fact]
    public async Task Runs_Once_On_Start_Then_Every_Interval()
    {
        PurgeReturns(new PurgeResult(0, 0, 0));
        using var worker = CreateWorker();

        await worker.StartAsync(CancellationToken.None).WaitAsync(WaitTimeout);
        await WaitForRunsAsync(1);

        _clock.Advance(TimeSpan.FromMilliseconds(_options.IntervalMs - 1));
        await Task.Delay(100);
        Assert.Equal(1, Volatile.Read(ref _runs));

        _clock.Advance(TimeSpan.FromMilliseconds(1));
        await WaitForRunsAsync(2);

        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Logs_Error_And_Keeps_Running_When_A_Run_Fails()
    {
        _purger
            .Setup(p => p.PurgeAsync(It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref _runs) == 1
                ? Task.FromException<PurgeResult>(new InvalidOperationException("database down"))
                : Task.FromResult(new PurgeResult(0, 0, 0)));
        using var worker = CreateWorker();

        await worker.StartAsync(CancellationToken.None);
        await WaitForRunsAsync(1);
        _clock.Advance(TimeSpan.FromMilliseconds(_options.IntervalMs));
        await WaitForRunsAsync(2);
        await worker.StopAsync(CancellationToken.None);

        Assert.False(worker.ExecuteTask!.IsFaulted);
        Assert.Contains(_logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Retention purge failed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Logs_Information_When_Something_Was_Deleted()
    {
        PurgeReturns(new PurgeResult(3, 1, 2));
        using var worker = CreateWorker();

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => _logger.Entries.Count > 0);
        await worker.StopAsync(CancellationToken.None);

        var (level, message) = Assert.Single(_logger.Entries);
        Assert.Equal(LogLevel.Information, level);
        Assert.Equal(
            "Retention purge deleted 3 outbox entries, 1 dead letters, 2 cancellation markers.",
            message);
    }

    [Fact]
    public async Task Logs_Debug_When_Nothing_Was_Deleted()
    {
        PurgeReturns(new PurgeResult(0, 0, 0));
        using var worker = CreateWorker();

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => _logger.Entries.Count > 0);
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal(LogLevel.Debug, Assert.Single(_logger.Entries).Level);
    }

    [Fact]
    public async Task Stops_Cleanly_On_Cancellation()
    {
        PurgeReturns(new PurgeResult(0, 0, 0));
        using var worker = CreateWorker();
        await worker.StartAsync(CancellationToken.None);
        await WaitForRunsAsync(1);

        await worker.StopAsync(CancellationToken.None);

        Assert.True(worker.ExecuteTask!.IsCompletedSuccessfully);
    }

    private void PurgeReturns(PurgeResult result) =>
        _purger
            .Setup(p => p.PurgeAsync(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref _runs);
                return Task.FromResult(result);
            });

    private Task WaitForRunsAsync(int expected) => WaitUntilAsync(() => Volatile.Read(ref _runs) >= expected);

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met within the timeout.");
            await Task.Delay(10);
        }
    }

    private RetentionWorker CreateWorker() => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        Options.Create(_options),
        _clock,
        _logger);
}
