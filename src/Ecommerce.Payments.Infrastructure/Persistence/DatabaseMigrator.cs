using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Applies pending EF Core migrations when the worker starts, before any consumer, the outbox
/// relay or the retention worker runs.
///
/// It is an <see cref="IHostedService"/> whose <see cref="StartAsync"/> does the work. The host
/// starts hosted services one at a time, in registration order, and stops at the first one that
/// fails. So it must be registered (<see cref="DatabaseMigrationServiceCollectionExtensions.AddDatabaseMigration"/>)
/// before every other hosted service.
///
/// While the database is unreachable (a transient Npgsql error, e.g. connection refused or the
/// server still starting), it retries up to <see cref="DatabaseMigrationOptions.MaxAttempts"/>
/// times, <see cref="DatabaseMigrationOptions.RetryDelayMs"/> apart. Any other failure (wrong
/// credentials, a failing migration) is not retried. Either way it logs Critical and throws
/// <see cref="DatabaseMigrationException"/>, so the host stops instead of consuming against a
/// missing or outdated schema. Concurrent instances are safe: EF Core takes a migration lock.
/// </summary>
public sealed class DatabaseMigrator : IHostedService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly DatabaseMigrationOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<DatabaseMigrator> _logger;

    /// <summary>Creates the migrator.</summary>
    public DatabaseMigrator(
        IServiceScopeFactory scopeFactory,
        IOptions<DatabaseMigrationOptions> options,
        TimeProvider timeProvider,
        ILogger<DatabaseMigrator> logger)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => MigrateAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Applies pending migrations, retrying while the database is unreachable.</summary>
    /// <exception cref="DatabaseMigrationException">The schema could not be migrated.</exception>
    public async Task MigrateAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Database migration at startup is disabled (DatabaseMigration:Enabled = false).");
            return;
        }

        var delay = TimeSpan.FromMilliseconds(_options.RetryDelayMs);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await ApplyAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (IsTransient(ex) && attempt < _options.MaxAttempts)
            {
                _logger.LogWarning(
                    "Database unreachable (attempt {Attempt}/{MaxAttempts}): {Reason}. Retrying in {DelayMs} ms.",
                    attempt,
                    _options.MaxAttempts,
                    ex.Message,
                    _options.RetryDelayMs);
                await Task.Delay(delay, _timeProvider, cancellationToken);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                _logger.LogCritical(
                    ex,
                    "Database unreachable after {MaxAttempts} attempts; migrations not applied. Stopping: check ConnectionStrings__Payments and that PostgreSQL is running.",
                    _options.MaxAttempts);
                throw new DatabaseMigrationException(
                    $"Database unreachable after {_options.MaxAttempts} attempts.", ex);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogCritical(ex, "Applying database migrations failed. Stopping.");
                throw new DatabaseMigrationException("Applying database migrations failed.", ex);
            }
        }
    }

    private async Task ApplyAsync(CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var database = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>().Database;

        var pending = (await database.GetPendingMigrationsAsync(cancellationToken)).ToList();
        if (pending.Count == 0)
        {
            _logger.LogInformation("Database schema is up to date; no migrations to apply.");
            return;
        }

        _logger.LogInformation(
            "Applying {Count} database migration(s): {Migrations}.",
            pending.Count,
            string.Join(", ", pending));
        await database.MigrateAsync(cancellationToken);
        _logger.LogInformation("Database migrations applied.");
    }

    /// <summary>
    /// Connection-level failures worth waiting out: Npgsql marks socket/timeout errors and
    /// server states such as "the database system is starting up" as transient.
    /// </summary>
    internal static bool IsTransient(Exception ex) =>
        ex is NpgsqlException { IsTransient: true } or TimeoutException;
}
