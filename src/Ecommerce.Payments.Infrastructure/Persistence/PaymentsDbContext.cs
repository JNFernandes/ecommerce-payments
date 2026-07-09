using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core context for the payments schema — the <c>payments</c> table (with its idempotency
/// unique index on <c>order_id</c>) and the <c>payment_dead_letters</c> table.
/// </summary>
public sealed class PaymentsDbContext : DbContext
{
    public PaymentsDbContext(DbContextOptions<PaymentsDbContext> options)
        : base(options)
    {
    }

    public DbSet<PaymentEntity> Payments => Set<PaymentEntity>();

    public DbSet<PaymentDeadLetterEntity> PaymentDeadLetters => Set<PaymentDeadLetterEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PaymentEntity>(payment =>
        {
            payment.ToTable("payments");
            payment.HasKey(p => p.Id);
            payment.Property(p => p.Status).HasConversion<string>().HasMaxLength(20);
            payment.Property(p => p.Currency).HasMaxLength(3);
            payment.HasIndex(p => p.OrderId).IsUnique();
        });

        modelBuilder.Entity<PaymentDeadLetterEntity>(deadLetter =>
        {
            deadLetter.ToTable("payment_dead_letters");
            deadLetter.HasKey(d => d.Id);
            deadLetter.Property(d => d.Payload).HasColumnType("jsonb");
        });
    }
}
