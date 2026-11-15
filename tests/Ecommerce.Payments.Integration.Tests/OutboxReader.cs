using System.Text.Json;
using Ecommerce.Payments.Infrastructure.Messaging.IntegrationEvents;
using Ecommerce.Payments.Infrastructure.Persistence;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Ecommerce.Payments.Integration.Tests;

/// <summary>
/// Reads the events recorded in <c>payment_outbox</c> (US-07). Integration tests exercise
/// Consumer → Repository against a real PostgreSQL container; the real
/// <see cref="Infrastructure.Outbox.PaymentOutbox"/> writes the entries, and Kafka publishing
/// (the relay) is out of scope at this tier. Each property reads committed rows afresh.
/// </summary>
public sealed class OutboxReader
{
    private readonly IServiceScopeFactory _scopeFactory;

    /// <summary>Creates the reader; register it as a singleton.</summary>
    public OutboxReader(IServiceScopeFactory scopeFactory)
    {
        _scopeFactory = scopeFactory;
    }

    /// <summary>Recorded <c>PaymentProcessed</c> events.</summary>
    public IReadOnlyList<PaymentProcessedEvent> Processed => Read<PaymentProcessedEvent>();

    /// <summary>Recorded <c>PaymentFailed</c> events.</summary>
    public IReadOnlyList<PaymentFailedEvent> Failed => Read<PaymentFailedEvent>();

    /// <summary>Recorded <c>PaymentRefunded</c> events.</summary>
    public IReadOnlyList<PaymentRefundedEvent> Refunded => Read<PaymentRefundedEvent>();

    /// <summary>All outbox entries, in recording order.</summary>
    public IReadOnlyList<OutboxEntryEntity> Entries
    {
        get
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
            return dbContext.PaymentOutbox.AsNoTracking().OrderBy(e => e.Sequence).ToList();
        }
    }

    private List<TEvent> Read<TEvent>()
    {
        var eventType = typeof(TEvent).Name;
        using var scope = _scopeFactory.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
        return dbContext.PaymentOutbox
            .AsNoTracking()
            .Where(e => e.EventType == eventType)
            .OrderBy(e => e.Sequence)
            .Select(e => e.Payload)
            .ToList()
            .Select(payload => JsonSerializer.Deserialize<TEvent>(payload)!)
            .ToList();
    }
}
