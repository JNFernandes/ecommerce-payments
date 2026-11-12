using Ecommerce.Payments.Infrastructure.DeadLetters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace Ecommerce.Payments.Infrastructure.Tests.DeadLetters;

public sealed class DeadLetterRetryWorkerTests : IDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 11, 12, 10, 0, 0, TimeSpan.Zero));
    private readonly Mock<IDeadLetterRetryProcessor> _processor = new();
    private readonly DeadLetterRetryOptions _options = new() { IntervalMs = 30_000 };
    private readonly ServiceProvider _services;
    private int _cycles;

    public DeadLetterRetryWorkerTests()
    {
        _services = new ServiceCollection()
            .AddScoped(_ => _processor.Object)
            .BuildServiceProvider();
    }

    public void Dispose() => _services.Dispose();

    [Fact]
    public async Task Should_Return_From_StartAsync_Without_Running_A_Cycle_Synchronously()
    {
        var neverCompletes = new TaskCompletionSource<int>();
        _processor.Setup(p => p.RunCycleAsync(It.IsAny<CancellationToken>())).Returns(neverCompletes.Task);
        using var worker = CreateWorker();

        // Throws TimeoutException if StartAsync waits for the (never-ending) retry cycle.
        await worker.StartAsync(CancellationToken.None).WaitAsync(WaitTimeout);
        await WaitUntilAsync(() => _processor.Invocations.Count == 1);
        neverCompletes.SetResult(0);
        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Should_Run_Next_Cycle_After_Interval_Elapses()
    {
        CountCycles();
        using var worker = CreateWorker();

        await worker.StartAsync(CancellationToken.None);
        await WaitForCyclesAsync(1);

        _clock.Advance(TimeSpan.FromMilliseconds(_options.IntervalMs - 1));
        await Task.Delay(100);
        Assert.Equal(1, Volatile.Read(ref _cycles));

        _clock.Advance(TimeSpan.FromMilliseconds(1));
        await WaitForCyclesAsync(2);

        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Should_Keep_Running_After_A_Cycle_Throws()
    {
        _processor
            .Setup(p => p.RunCycleAsync(It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref _cycles) == 1
                ? Task.FromException<int>(new InvalidOperationException("database down"))
                : Task.FromResult(0));
        using var worker = CreateWorker();

        await worker.StartAsync(CancellationToken.None);
        await WaitForCyclesAsync(1);
        _clock.Advance(TimeSpan.FromMilliseconds(_options.IntervalMs));
        await WaitForCyclesAsync(2);

        Assert.False(worker.ExecuteTask!.IsFaulted);
        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Should_Stop_When_Cancellation_Is_Requested()
    {
        CountCycles();
        using var worker = CreateWorker();
        await worker.StartAsync(CancellationToken.None);
        await WaitForCyclesAsync(1);

        await worker.StopAsync(CancellationToken.None);

        Assert.True(worker.ExecuteTask!.IsCompletedSuccessfully);
    }

    private void CountCycles() =>
        _processor
            .Setup(p => p.RunCycleAsync(It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult(Interlocked.Increment(ref _cycles)));

    private Task WaitForCyclesAsync(int expected) => WaitUntilAsync(() => Volatile.Read(ref _cycles) >= expected);

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met within the timeout.");
            await Task.Delay(10);
        }
    }

    private DeadLetterRetryWorker CreateWorker() => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        Options.Create(_options),
        _clock,
        NullLogger<DeadLetterRetryWorker>.Instance);
}
