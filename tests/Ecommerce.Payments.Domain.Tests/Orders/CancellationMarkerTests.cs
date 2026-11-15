using Ecommerce.Payments.Domain.Orders;

namespace Ecommerce.Payments.Domain.Tests.Orders;

public class CancellationMarkerTests
{
    private static readonly DateTimeOffset CancelledAt = new(2026, 11, 15, 9, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset RecordedAt = new(2026, 11, 15, 9, 0, 2, TimeSpan.Zero);

    [Fact]
    public void Record_Keeps_Every_Value()
    {
        var orderId = Guid.NewGuid();
        var eventId = Guid.NewGuid();

        var marker = CancellationMarker.Record(orderId, eventId, CancelledAt, RecordedAt, "stock rejected");

        Assert.Equal(orderId, marker.OrderId);
        Assert.Equal(eventId, marker.SourceEventId);
        Assert.Equal(CancelledAt, marker.CancelledAt);
        Assert.Equal(RecordedAt, marker.RecordedAt);
        Assert.Equal("stock rejected", marker.Reason);
    }

    [Fact]
    public void Record_Allows_Null_Reason()
    {
        var marker = CancellationMarker.Record(Guid.NewGuid(), Guid.NewGuid(), CancelledAt, RecordedAt, null);

        Assert.Null(marker.Reason);
    }

    [Fact]
    public void Record_With_Empty_OrderId_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => CancellationMarker.Record(Guid.Empty, Guid.NewGuid(), CancelledAt, RecordedAt, null));

        Assert.Equal("orderId", ex.ParamName);
    }

    [Fact]
    public void Record_With_Empty_SourceEventId_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(
            () => CancellationMarker.Record(Guid.NewGuid(), Guid.Empty, CancelledAt, RecordedAt, null));

        Assert.Equal("sourceEventId", ex.ParamName);
    }

    [Fact]
    public void Rehydrate_Restores_Every_Value_Without_Validation()
    {
        var marker = CancellationMarker.Rehydrate(Guid.Empty, Guid.Empty, CancelledAt, RecordedAt, "r");

        Assert.Equal(Guid.Empty, marker.OrderId);
        Assert.Equal(Guid.Empty, marker.SourceEventId);
        Assert.Equal(CancelledAt, marker.CancelledAt);
        Assert.Equal(RecordedAt, marker.RecordedAt);
        Assert.Equal("r", marker.Reason);
    }
}
