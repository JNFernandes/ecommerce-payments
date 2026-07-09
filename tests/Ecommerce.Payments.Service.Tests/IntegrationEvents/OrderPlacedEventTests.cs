using Ecommerce.Payments.Service.IntegrationEvents;

namespace Ecommerce.Payments.Service.Tests.IntegrationEvents;

public class OrderPlacedEventTests
{
    // Matches the real payload shape from ecommerce-orders/src/domain/events/order-placed.event.ts:
    // no separate orderId (aggregateId is the order id), no currency, amount is "totalAmount".
    // "items" is included to prove extra, unrecognized fields don't break parsing.
    private const string ValidJson = """
        {
          "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
          "occurredAt": "2026-07-09T14:32:00Z",
          "aggregateId": "b3f1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
          "version": 1,
          "customerId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
          "items": [{ "productId": "d1a2b3c4-5e6f-7a8b-9c0d-1e2f3a4b5c6d", "quantity": 2, "unitPrice": 64.995 }],
          "totalAmount": 129.99
        }
        """;

    [Fact]
    public void TryParse_WellFormedPayload_ReturnsTrueWithAllFieldsMapped()
    {
        var result = OrderPlacedEvent.TryParse(ValidJson, out var orderPlaced, out var error);

        Assert.True(result);
        Assert.Null(error);
        Assert.NotNull(orderPlaced);
        Assert.Equal(Guid.Parse("3fa85f64-5717-4562-b3fc-2c963f66afa6"), orderPlaced.EventId);
        Assert.Equal(Guid.Parse("b3f1c2d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d"), orderPlaced.AggregateId);
        Assert.Equal(Guid.Parse("7c9e6679-7425-40de-944b-e07fc1f90ae7"), orderPlaced.CustomerId);
        Assert.Equal(1, orderPlaced.Version);
        Assert.Equal(129.99m, orderPlaced.TotalAmount);
    }

    [Fact]
    public void TryParse_NotJson_ReturnsFalse()
    {
        var result = OrderPlacedEvent.TryParse("this is not json", out var orderPlaced, out var error);

        Assert.False(result);
        Assert.Null(orderPlaced);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData("eventId")]
    [InlineData("aggregateId")]
    [InlineData("customerId")]
    [InlineData("version")]
    [InlineData("occurredAt")]
    [InlineData("totalAmount")]
    public void TryParse_MissingRequiredField_ReturnsFalse(string fieldToRemove)
    {
        var json = RemoveField(ValidJson, fieldToRemove);

        var result = OrderPlacedEvent.TryParse(json, out var orderPlaced, out var error);

        Assert.False(result);
        Assert.Null(orderPlaced);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-129.99)]
    public void TryParse_NonPositiveTotalAmount_ReturnsFalse(decimal totalAmount)
    {
        var json = ValidJson.Replace("129.99", totalAmount.ToString(System.Globalization.CultureInfo.InvariantCulture));

        var result = OrderPlacedEvent.TryParse(json, out var orderPlaced, out var error);

        Assert.False(result);
        Assert.Null(orderPlaced);
        Assert.NotNull(error);
    }

    private static string RemoveField(string json, string fieldName)
    {
        var lines = json.Split('\n')
            .Where(line => !line.TrimStart().StartsWith($"\"{fieldName}\"", StringComparison.Ordinal))
            .ToArray();
        return string.Join('\n', lines);
    }
}
