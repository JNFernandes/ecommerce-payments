using Polly;
using Polly.Retry;

namespace Ecommerce.Payments.Service.Payments;

/// <summary>
/// Shared bounded retry for transient PostgreSQL failures in the Service layer handlers:
/// 3 retries with exponential backoff starting at 200 ms (Constitution Principle II). If it is
/// still failing after that, the handler logs and rethrows so the Consumer does not commit.
/// </summary>
internal static class PersistenceRetry
{
    /// <summary>The retry pipeline; stateless and safe to share.</summary>
    public static ResiliencePipeline Pipeline { get; } = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            MaxRetryAttempts = 3,
            Delay = TimeSpan.FromMilliseconds(200),
            BackoffType = DelayBackoffType.Exponential
        })
        .Build();
}
