using Ecommerce.Payments.Domain.Orders;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Orders;
using Microsoft.EntityFrameworkCore;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core implementation of <see cref="ICancellationMarkerRepository"/>. Maps explicitly between
/// <see cref="CancellationMarker"/> and <see cref="CancellationMarkerEntity"/> (Constitution
/// Principle I). Runs inside the caller's order transaction because it shares the scoped
/// <see cref="PaymentsDbContext"/> with <see cref="OrderUnitOfWork"/>.
/// </summary>
public sealed class CancellationMarkerRepository : ICancellationMarkerRepository
{
    private readonly PaymentsDbContext _dbContext;

    /// <summary>Creates the repository over a scoped <see cref="PaymentsDbContext"/>.</summary>
    public CancellationMarkerRepository(PaymentsDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    /// <inheritdoc />
    public async Task<CancellationMarker?> GetByOrderIdAsync(Guid orderId, CancellationToken cancellationToken)
    {
        var entity = await _dbContext.CancellationMarkers
            .AsNoTracking()
            .SingleOrDefaultAsync(m => m.OrderId == orderId, cancellationToken);

        return entity is null
            ? null
            : CancellationMarker.Rehydrate(
                entity.OrderId,
                entity.SourceEventId,
                entity.CancelledAt,
                entity.RecordedAt,
                entity.Reason);
    }

    /// <inheritdoc />
    public async Task AddAsync(CancellationMarker marker, CancellationToken cancellationToken)
    {
        _dbContext.CancellationMarkers.Add(new CancellationMarkerEntity
        {
            OrderId = marker.OrderId,
            SourceEventId = marker.SourceEventId,
            CancelledAt = marker.CancelledAt,
            RecordedAt = marker.RecordedAt,
            Reason = marker.Reason
        });
        await _dbContext.SaveChangesAsync(cancellationToken);
    }
}
