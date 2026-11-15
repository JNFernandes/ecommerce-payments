using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core context for the payments schema — the <c>payments</c> table (with its idempotency
/// unique index on <c>order_id</c>), the <c>payment_outbox</c> table (outbound events with
/// their relay bookkeeping, a recording sequence and a partial due-row index; ADR 0006), the <c>order_cancellation_markers</c>
/// table (primary key <c>OrderId</c>: one marker per order), and the <c>consumed_dead_letters</c>
/// table (inbound messages that could not be handled; unique per Kafka position).
/// </summary>
public sealed class PaymentsDbContext : DbContext
{
    /// <summary>Creates the context with the given options.</summary>
    public PaymentsDbContext(DbContextOptions<PaymentsDbContext> options)
        : base(options)
    {
    }

    /// <summary>The <c>payments</c> table.</summary>
    public DbSet<PaymentEntity> Payments => Set<PaymentEntity>();

    /// <summary>The <c>payment_outbox</c> table: outbound payment events and their delivery state.</summary>
    public DbSet<OutboxEntryEntity> PaymentOutbox => Set<OutboxEntryEntity>();

    /// <summary>The <c>order_cancellation_markers</c> table: orders cancelled before their payment existed.</summary>
    public DbSet<CancellationMarkerEntity> CancellationMarkers => Set<CancellationMarkerEntity>();

    /// <summary>The <c>consumed_dead_letters</c> table: inbound messages that could not be handled.</summary>
    public DbSet<ConsumedDeadLetterEntity> ConsumedDeadLetters => Set<ConsumedDeadLetterEntity>();

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

        modelBuilder.Entity<OutboxEntryEntity>(entry =>
        {
            entry.ToTable("payment_outbox");
            entry.HasKey(e => e.Id);
            entry.Property(e => e.Sequence).UseIdentityByDefaultColumn();
            entry.HasIndex(e => e.Sequence).IsUnique().HasDatabaseName("IX_payment_outbox_sequence");
            entry.HasIndex(e => e.EventId).IsUnique().HasDatabaseName("IX_payment_outbox_event_id");
            entry.Property(e => e.Payload).HasColumnType("jsonb");
            entry.Property(e => e.Status).HasConversion<string>().HasMaxLength(20);
            // Keeps the relay's claim query cheap as published entries accumulate.
            entry.HasIndex(e => e.NextAttemptAt)
                .HasDatabaseName("IX_payment_outbox_due")
                .HasFilter("\"Status\" = 'Pending'");
            // Per-payment ordering check (an earlier unpublished entry blocks later ones).
            entry.HasIndex(e => new { e.PaymentId, e.Sequence })
                .HasDatabaseName("IX_payment_outbox_payment_sequence");
        });

        modelBuilder.Entity<CancellationMarkerEntity>(marker =>
        {
            marker.ToTable("order_cancellation_markers");
            marker.HasKey(m => m.OrderId);
        });

        modelBuilder.Entity<ConsumedDeadLetterEntity>(deadLetter =>
        {
            deadLetter.ToTable(
                "consumed_dead_letters",
                table => table.HasCheckConstraint("CK_consumed_dead_letters_attempts", "\"Attempts\" >= 1"));
            deadLetter.HasKey(d => d.Id);
            // One row per Kafka position: storing the same message twice is a no-op.
            deadLetter.HasIndex(d => new { d.Topic, d.Partition, d.Offset })
                .IsUnique()
                .HasDatabaseName("IX_consumed_dead_letters_position");
        });
    }
}
