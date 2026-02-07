using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace NStore.Core.Tests
{
    public class AsyncParallelExtensionsTests
    {
        [Fact]
        public async Task foreach_async_should_wait_for_running_tasks_before_throwing_cancellation()
        {
            var source = new[] { 1, 2, 3 };
            var firstItemStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirstItem = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var cts = new CancellationTokenSource();

            var execution = AsyncParallelExtensions.ForEachAsync(
                source,
                maxDegreeOfParallelism: 1,
                async (item, _) =>
                {
                    if (item == 1)
                    {
                        firstItemStarted.TrySetResult(true);
                        await releaseFirstItem.Task.ConfigureAwait(false);
                    }
                },
                cts.Token);

            await firstItemStarted.Task.ConfigureAwait(false);
            cts.Cancel();

            await Task.Delay(100).ConfigureAwait(false);
            Assert.False(execution.IsCompleted);

            releaseFirstItem.TrySetResult(true);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await execution.ConfigureAwait(false));
        }

        [Fact]
        public async Task foreach_async_should_aggregate_worker_exceptions()
        {
            var source = new[] { 1, 2, 3 };

            var error = await Assert.ThrowsAsync<AggregateException>(async () =>
                await AsyncParallelExtensions.ForEachAsync(
                        source,
                        maxDegreeOfParallelism: 3,
                        (item, _) =>
                        {
                            if (item % 2 == 0)
                            {
                                throw new InvalidOperationException($"boom-{item}");
                            }

                            return Task.CompletedTask;
                        },
                        CancellationToken.None)
                    .ConfigureAwait(false))
                .ConfigureAwait(false);

            Assert.Contains(error.InnerExceptions, x =>
                x is InvalidOperationException && x.Message == "boom-2");
        }
    }
}
