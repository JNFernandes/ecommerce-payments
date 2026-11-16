using Ecommerce.Payments.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.PostgreSql;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// <see cref="DatabaseMigrator"/> against a real PostgreSQL container: migrates an empty
/// database before any other hosted service starts, is a no-op when up to date, retries while
/// the database is unreachable and then fails fast with a Critical log, and never retries a
/// non-transient failure such as wrong credentials.
/// </summary>
public class DatabaseMigratorTests : IAsyncLifetime
{
    // Nothing listens on port 1; Timeout bounds each attempt.
    private const string UnreachableConnectionString =
        "Host=127.0.0.1;Port=1;Database=payments;Username=x;Password=x;Timeout=1";

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly ListLogger _logger = new();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Should_Apply_All_Migrations_To_An_Empty_Database()
    {
        await CreateMigrator(_postgres.GetConnectionString()).MigrateAsync(CancellationToken.None);

        await using var context = CreateDbContext(_postgres.GetConnectionString());
        Assert.Empty(await context.Database.GetPendingMigrationsAsync());
        Assert.Equal(0, await context.PaymentOutbox.CountAsync());
        Assert.Contains(_logger.Entries, e => e.Level == LogLevel.Information
            && e.Message.StartsWith("Applying ", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Should_Do_Nothing_When_Schema_Is_Up_To_Date()
    {
        await CreateMigrator(_postgres.GetConnectionString()).MigrateAsync(CancellationToken.None);
        _logger.Clear();

        await CreateMigrator(_postgres.GetConnectionString()).MigrateAsync(CancellationToken.None);

        var entry = Assert.Single(_logger.Entries);
        Assert.Equal("Database schema is up to date; no migrations to apply.", entry.Message);
    }

    [Fact]
    public async Task Should_Retry_Then_Fail_With_Critical_Log_When_Database_Is_Unreachable()
    {
        var migrator = CreateMigrator(UnreachableConnectionString, maxAttempts: 3);

        await Assert.ThrowsAsync<DatabaseMigrationException>(() => migrator.MigrateAsync(CancellationToken.None));

        Assert.Equal(2, _logger.Entries.Count(e => e.Level == LogLevel.Warning));
        Assert.Contains(_logger.Entries, e => e.Level == LogLevel.Critical
            && e.Message.Contains("Database unreachable after 3 attempts", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Should_Fail_Without_Retrying_When_Credentials_Are_Wrong()
    {
        var connectionString = new Npgsql.NpgsqlConnectionStringBuilder(_postgres.GetConnectionString())
        {
            Password = "wrong-password"
        }.ConnectionString;
        var migrator = CreateMigrator(connectionString, maxAttempts: 5);

        await Assert.ThrowsAsync<DatabaseMigrationException>(() => migrator.MigrateAsync(CancellationToken.None));

        Assert.DoesNotContain(_logger.Entries, e => e.Level == LogLevel.Warning);
        Assert.Contains(_logger.Entries, e => e.Level == LogLevel.Critical
            && e.Message.StartsWith("Applying database migrations failed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Should_Skip_When_Disabled()
    {
        var migrator = CreateMigrator(UnreachableConnectionString, enabled: false);

        await migrator.MigrateAsync(CancellationToken.None);

        Assert.Contains("disabled", Assert.Single(_logger.Entries).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Should_Migrate_Before_Later_Hosted_Services_Start()
    {
        var probe = new ProbeHostedService(_postgres.GetConnectionString());
        using var host = BuildHost(_postgres.GetConnectionString(), probe);

        await host.StartAsync();
        await host.StopAsync();

        Assert.True(probe.Started);
        Assert.True(probe.SchemaWasMigratedAtStart);
    }

    [Fact]
    public async Task Should_Stop_Host_Before_Later_Hosted_Services_Start_When_Migration_Fails()
    {
        var probe = new ProbeHostedService(UnreachableConnectionString);
        using var host = BuildHost(UnreachableConnectionString, probe, maxAttempts: 2);

        await Assert.ThrowsAsync<DatabaseMigrationException>(() => host.StartAsync());

        Assert.False(probe.Started);
    }

    // ---- Helpers ----

    private DatabaseMigrator CreateMigrator(string connectionString, int maxAttempts = 3, bool enabled = true)
    {
        var services = new ServiceCollection()
            .AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(connectionString))
            .BuildServiceProvider();

        return new DatabaseMigrator(
            services.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new DatabaseMigrationOptions { Enabled = enabled, MaxAttempts = maxAttempts, RetryDelayMs = 10 }),
            TimeProvider.System,
            _logger);
    }

    private static IHost BuildHost(string connectionString, ProbeHostedService probe, int maxAttempts = 3)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<DatabaseMigrationOptions>(o =>
        {
            o.MaxAttempts = maxAttempts;
            o.RetryDelayMs = 10;
        });
        builder.Services.AddDbContext<PaymentsDbContext>(o => o.UseNpgsql(connectionString));
        builder.Services.AddDatabaseMigration();
        builder.Services.AddHostedService(_ => probe);
        return builder.Build();
    }

    private static PaymentsDbContext CreateDbContext(string connectionString) =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>().UseNpgsql(connectionString).Options);

    /// <summary>Stands in for a consumer: records whether it started and what schema it saw.</summary>
    private sealed class ProbeHostedService(string connectionString) : IHostedService
    {
        public bool Started { get; private set; }

        public bool SchemaWasMigratedAtStart { get; private set; }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Started = true;
            await using var context = CreateDbContext(connectionString);
            SchemaWasMigratedAtStart = !(await context.Database.GetPendingMigrationsAsync(cancellationToken)).Any();
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class ListLogger : ILogger<DatabaseMigrator>
    {
        private readonly Lock _gate = new();
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public List<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (_gate)
                {
                    return [.. _entries];
                }
            }
        }

        public void Clear()
        {
            lock (_gate)
            {
                _entries.Clear();
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_gate)
            {
                _entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
