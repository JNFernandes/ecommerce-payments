using Ecommerce.Payments.Service.IntegrationEvents;

namespace Ecommerce.Payments.Service.Tests.IntegrationEvents;

public class OrderCancelledEventTests
{
    // Matches the payload from ecommerce-orders/src/domain/events/order-cancelled.event.ts.
    // "extra" proves unknown fields don't break parsing. Every field sits on its own line so
    // RemoveField can drop it.
    private const string ValidJson = """
        {
          "eventId": "6f1d2c3b-1111-4000-8000-000000000001",
          "occurredAt": "2026-11-12T10:05:00.000Z",
          "aggregateId": "9d2b6e11-2222-4000-8000-000000000004",
          "version": 1,
          "customerId": "1a2b3c4d-3333-4000-8000-000000000005",
          "reason": "Payment failed: amount exceeds threshold",
          "extra": "ignored"
        }
        """;

    [Fact]
    public void TryParse_WellFormedPayload_ReturnsTrueWithAllFieldsMapped()
    {
        var result = OrderCancelledEvent.TryParse(ValidJson, out var orderCancelled, out var error);

        Assert.True(result);
        Assert.Null(error);
        Assert.NotNull(orderCancelled);
        Assert.Equal(Guid.Parse("6f1d2c3b-1111-4000-8000-000000000001"), orderCancelled.EventId);
        Assert.Equal(Guid.Parse("9d2b6e11-2222-4000-8000-000000000004"), orderCancelled.AggregateId);
        Assert.Equal(Guid.Parse("1a2b3c4d-3333-4000-8000-000000000005"), orderCancelled.CustomerId);
        Assert.Equal(1, orderCancelled.Version);
        Assert.Equal(new DateTimeOffset(2026, 11, 12, 10, 5, 0, TimeSpan.Zero), orderCancelled.OccurredAt);
        Assert.Equal("Payment failed: amount exceeds threshold", orderCancelled.Reason);
    }

    [Theory]
    [InlineData("customerId")]
    [InlineData("reason")]
    public void TryParse_OptionalFieldAbsent_ReturnsTrue(string fieldToRemove)
    {
        var result = OrderCancelledEvent.TryParse(RemoveField(ValidJson, fieldToRemove), out var orderCancelled, out var error);

        Assert.True(result, error);
        Assert.NotNull(orderCancelled);
    }

    [Theory]
    [InlineData("eventId")]
    [InlineData("aggregateId")]
    [InlineData("version")]
    [InlineData("occurredAt")]
    public void TryParse_MissingRequiredField_ReturnsFalseNamingTheField(string fieldToRemove)
    {
        var result = OrderCancelledEvent.TryParse(RemoveField(ValidJson, fieldToRemove), out var orderCancelled, out var error);

        Assert.False(result);
        Assert.Null(orderCancelled);
        Assert.NotNull(error);
        Assert.Contains(fieldToRemove, error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"eventId\": \"6f1d2c3b-1111-4000-8000-000000000001\"", "\"eventId\": \"00000000-0000-0000-0000-000000000000\"", "eventId")]
    [InlineData("\"aggregateId\": \"9d2b6e11-2222-4000-8000-000000000004\"", "\"aggregateId\": \"00000000-0000-0000-0000-000000000000\"", "aggregateId")]
    [InlineData("\"version\": 1", "\"version\": 0", "version")]
    public void TryParse_EmptyOrNonPositiveRequiredField_ReturnsFalse(string original, string replacement, string field)
    {
        var result = OrderCancelledEvent.TryParse(ValidJson.Replace(original, replacement, StringComparison.Ordinal), out _, out var error);

        Assert.False(result);
        Assert.Contains(field, error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("this is not json")]
    [InlineData("null")]
    [InlineData("")]
    public void TryParse_MalformedOrEmptyBody_ReturnsFalse(string body)
    {
        var result = OrderCancelledEvent.TryParse(body, out var orderCancelled, out var error);

        Assert.False(result);
        Assert.Null(orderCancelled);
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
