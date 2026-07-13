using Ecommerce.Payments.Domain.Payments;
using Ecommerce.Payments.Infrastructure.Persistence.Entities;
using Ecommerce.Payments.Service.Payments;
using Microsoft.EntityFrameworkCore;

namespace Ecommerce.Payments.Infrastructure.Persistence;

/// <summary>
/// EF Core implementation of <see cref="IPaymentRepository"/>. Maps explicitly between the
/// <see cref="Payment"/> domain aggregate and <see cref="PaymentEntity"/> so EF Core types never
/// leak into the Domain layer (Constitution Principle I). The <c>payments.order_id</c> unique
/// constraint (see the initial migration) is the durable idempotency guarantee.
/// </summary>
public sealed class PaymentRepository : IPaymentRepository
{
    private readonly PaymentsDbContext _dbContext;

    public PaymentRepository(PaymentsDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task SaveAsync(Payment payment, CancellationToken cancellationToken)
    {
        var entity = new PaymentEntity
        {
            Id = payment.Id,
            OrderId = payment.OrderId,
            CustomerId = payment.CustomerId,
            Amount = payment.Amount,
            Currency = payment.Currency,
            Status = payment.Status,
            SourceEventId = payment.SourceEventId,
            CreatedAt = payment.CreatedAt,
            ProcessedAt = payment.ProcessedAt,
            FailureReason = payment.FailureReason,
            FailedAt = payment.FailedAt
        };

        _dbContext.Payments.Add(entity);
        await _dbContext.SaveChangesAsync(cancellationToken);
    }

    public Task<bool> ExistsByOrderIdAsync(Guid orderId, CancellationToken cancellationToken) =>
        _dbContext.Payments.AnyAsync(p => p.OrderId == orderId, cancellationToken);
}
