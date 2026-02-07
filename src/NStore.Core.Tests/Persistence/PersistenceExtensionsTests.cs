using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NStore.Core.Persistence;
using Xunit;

namespace NStore.Core.Tests.Persistence
{
    public class PersistenceExtensionsTests
    {
        [Fact]
        public async Task append_batch_async_should_validate_arguments()
        {
            var queue = new[] { new WriteJob("p", 1, "payload", "op") };
            var options = new ParallelBatchAppendOptions { BatchSize = 1, MaxWriters = 1 };
            IEnhancedPersistence nullPersistence = null;
            var persistence = new Mock<IEnhancedPersistence>().Object;

            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                    await PersistenceExtensions.AppendBatchAsync(
                            nullPersistence,
                            queue,
                            options,
                            CancellationToken.None)
                        .ConfigureAwait(false))
                .ConfigureAwait(false);

            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                    await persistence.AppendBatchAsync(
                            null,
                            options,
                            CancellationToken.None)
                        .ConfigureAwait(false))
                .ConfigureAwait(false);

            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                    await persistence.AppendBatchAsync(
                            queue,
                            null,
                            CancellationToken.None)
                        .ConfigureAwait(false))
                .ConfigureAwait(false);

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                    await persistence.AppendBatchAsync(
                            queue,
                            new ParallelBatchAppendOptions { BatchSize = 0, MaxWriters = 1 },
                            CancellationToken.None)
                        .ConfigureAwait(false))
                .ConfigureAwait(false);

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                    await persistence.AppendBatchAsync(
                            queue,
                            new ParallelBatchAppendOptions { BatchSize = 1, MaxWriters = 0 },
                            CancellationToken.None)
                        .ConfigureAwait(false))
                .ConfigureAwait(false);
        }

        [Fact]
        public async Task append_batch_async_should_split_batches_and_bound_parallelism()
        {
            var queue = Enumerable.Range(1, 10)
                .Select(i => new WriteJob("p", i, $"payload-{i}", $"op-{i}"))
                .ToArray();
            var options = new ParallelBatchAppendOptions
            {
                BatchSize = 3,
                MaxWriters = 2
            };
            var observedBatchSizes = new ConcurrentBag<int>();
            var currentConcurrency = 0;
            var maxConcurrency = 0;
            var persistence = new Mock<IEnhancedPersistence>();

            persistence
                .Setup(x => x.AppendBatchAsync(It.IsAny<WriteJob[]>(), It.IsAny<CancellationToken>()))
                .Returns(async (WriteJob[] batch, CancellationToken ct) =>
                {
                    observedBatchSizes.Add(batch.Length);

                    var current = Interlocked.Increment(ref currentConcurrency);
                    UpdateMax(ref maxConcurrency, current);
                    try
                    {
                        await Task.Delay(30, ct).ConfigureAwait(false);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref currentConcurrency);
                    }

                    foreach (var job in batch)
                    {
                        job.Succeeded();
                    }
                });

            await persistence.Object.AppendBatchAsync(queue, options, CancellationToken.None).ConfigureAwait(false);

            persistence.Verify(
                x => x.AppendBatchAsync(It.IsAny<WriteJob[]>(), It.IsAny<CancellationToken>()),
                Times.Exactly(4));
            Assert.Equal(new[] { 1, 3, 3, 3 }, observedBatchSizes.OrderBy(x => x).ToArray());
            Assert.True(maxConcurrency <= options.MaxWriters);
            Assert.All(queue, x => Assert.Equal(WriteJob.WriteResult.Committed, x.Result));
        }

        [Fact]
        public async Task append_batch_async_should_propagate_underlying_errors()
        {
            var expected = new InvalidOperationException("boom");
            var queue = new[] { new WriteJob("p", 1, "payload", "op") };
            var options = new ParallelBatchAppendOptions { BatchSize = 1, MaxWriters = 1 };
            var persistence = new Mock<IEnhancedPersistence>();

            persistence
                .Setup(x => x.AppendBatchAsync(It.IsAny<WriteJob[]>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(expected);

            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
                    await persistence.Object.AppendBatchAsync(queue, options, CancellationToken.None).ConfigureAwait(false))
                .ConfigureAwait(false);

            if (exception is AggregateException aggregate)
            {
                Assert.Contains(aggregate.InnerExceptions, x => x is InvalidOperationException && x.Message == expected.Message);
                return;
            }

            var invalid = Assert.IsType<InvalidOperationException>(exception);
            Assert.Equal(expected.Message, invalid.Message);
        }

        [Fact]
        public async Task append_batch_async_should_propagate_cancellation()
        {
            var queue = new[] { new WriteJob("p", 1, "payload", "op") };
            var options = new ParallelBatchAppendOptions { BatchSize = 1, MaxWriters = 1 };
            var persistence = new Mock<IEnhancedPersistence>();
            using var cts = new CancellationTokenSource();

            persistence
                .Setup(x => x.AppendBatchAsync(It.IsAny<WriteJob[]>(), It.IsAny<CancellationToken>()))
                .Returns<WriteJob[], CancellationToken>((_, ct) => Task.Delay(Timeout.Infinite, ct));

            cts.CancelAfter(100);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                    await persistence.Object.AppendBatchAsync(queue, options, cts.Token).ConfigureAwait(false))
                .ConfigureAwait(false);
        }

        private static void UpdateMax(ref int target, int value)
        {
            while (true)
            {
                var snapshot = Volatile.Read(ref target);
                if (value <= snapshot)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref target, value, snapshot) == snapshot)
                {
                    return;
                }
            }
        }
    }
}
