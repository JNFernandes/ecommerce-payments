using Ecommerce.Payments.Infrastructure.Persistence;
using Npgsql;

namespace Ecommerce.Payments.Infrastructure.Tests.Persistence;

public class DatabaseMigrationOptionsTests
{
    [Fact]
    public void Defaults_Are_Valid_And_Enabled()
    {
        var options = new DatabaseMigrationOptions();

        Assert.True(options.IsValid());
        Assert.True(options.Enabled);
        Assert.Equal(10, options.MaxAttempts);
        Assert.Equal(3_000, options.RetryDelayMs);
    }

    [Theory]
    [InlineData(0, 3_000)]
    [InlineData(-1, 3_000)]
    [InlineData(10, 0)]
    [InlineData(10, -5)]
    public void Non_Positive_Attempts_Or_Delay_Are_Invalid(int maxAttempts, int retryDelayMs)
    {
        Assert.False(new DatabaseMigrationOptions { MaxAttempts = maxAttempts, RetryDelayMs = retryDelayMs }.IsValid());
    }

    [Fact]
    public void Timeouts_And_Socket_Errors_Are_Transient()
    {
        Assert.True(DatabaseMigrator.IsTransient(new TimeoutException()));
        Assert.True(DatabaseMigrator.IsTransient(new NpgsqlException("refused", new System.Net.Sockets.SocketException())));
    }

    [Fact]
    public void Other_Failures_Are_Not_Transient()
    {
        Assert.False(DatabaseMigrator.IsTransient(new InvalidOperationException("migration failed")));
        Assert.False(DatabaseMigrator.IsTransient(new NpgsqlException("bad")));
    }
}
