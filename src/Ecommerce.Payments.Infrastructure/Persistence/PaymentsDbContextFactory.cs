using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// Design-time factory so <c>dotnet ef migrations</c> can construct a <see cref="PaymentsDbContext"/>
/// directly against this project without needing the Consumer host wired up.
/// </summary>
public sealed class PaymentsDbContextFactory : IDesignTimeDbContextFactory<PaymentsDbContext>
{
    public PaymentsDbContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<PaymentsDbContext>();
        var connectionString = Environment.GetEnvironmentVariable("PAYMENTS_CONNECTION_STRING")
            ?? "Host=localhost;Port=5432;Database=payments;Username=payments;Password=payments";
        optionsBuilder.UseNpgsql(connectionString);
        return new PaymentsDbContext(optionsBuilder.Options);
    }
}
