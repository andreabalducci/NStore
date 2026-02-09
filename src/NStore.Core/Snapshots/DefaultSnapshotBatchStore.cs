using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Logging;

namespace NStore.Core.Snapshots
{
    /// <summary>
    /// Default implementation of <see cref="ISnapshotBatchStore"/> that performs batch
    /// snapshot operations in parallel using an underlying <see cref="ISnapshotStore"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This implementation wraps an existing <see cref="ISnapshotStore"/> and executes
    /// operations in parallel using a bounded async fan-out helper.
    /// It does not perform database-level batch operations; instead, it optimizes throughput
    /// by issuing multiple concurrent requests.
    /// </para>
    /// <para>
    /// This is a general-purpose implementation suitable for most scenarios. For specialized
    /// storage backends that support native batch queries (e.g., SQL with IN clauses, MongoDB
    /// with $in operator), consider implementing a custom <see cref="ISnapshotBatchStore"/>
    /// that leverages those capabilities for better performance.
    /// </para>
    /// </remarks>
    public class DefaultSnapshotBatchStore : ISnapshotBatchStore
    {
        private readonly ISnapshotStore _snapshotStore;
        private readonly INStoreLogger _logger;
        private readonly ConcurrentQueue<IReadOnlyDictionary<string, SnapshotInfo>> _pendingSnapshotWrites =
            new ConcurrentQueue<IReadOnlyDictionary<string, SnapshotInfo>>();
        private readonly SemaphoreSlim _pendingSnapshotSignal = new SemaphoreSlim(0);
        private readonly object _lifecycleLock = new object();
        private readonly Task _queueProcessor;

        private bool _isDisposing;
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultSnapshotBatchStore"/> class.
        /// </summary>
        /// <param name="snapshotStore">The underlying snapshot store to use for individual snapshot operations.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="snapshotStore"/> is null.</exception>
        public DefaultSnapshotBatchStore(ISnapshotStore snapshotStore, INStoreLoggerFactory loggerFactory = null)
        {
            _snapshotStore = snapshotStore ?? throw new ArgumentNullException(nameof(snapshotStore));
            _logger = (loggerFactory ?? NStoreNullLoggerFactory.Instance)
                .CreateLogger(typeof(DefaultSnapshotBatchStore).FullName);
            _queueProcessor = ProcessQueueAsync();
        }

        /// <summary>
        /// Retrieves the most recent snapshots for multiple partitions by executing
        /// parallel calls to <see cref="ISnapshotStore.GetLastAsync"/>.
        /// </summary>
        /// <param name="snapshotPartitionIds">Collection of partition IDs to retrieve snapshots for.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>
        /// A dictionary mapping partition IDs to their corresponding <see cref="SnapshotInfo"/>.
        /// Only partitions that have snapshots are included in the result.
        /// </returns>
        /// <remarks>
        /// <para>
        /// <strong>Implementation Details:</strong>
        /// </para>
        /// <list type="number">
        /// <item><description>Deduplicates the input partition IDs to avoid redundant queries.</description></item>
        /// <item><description>Executes <see cref="ISnapshotStore.GetLastAsync"/> for each partition in parallel.</description></item>
        /// <item><description>Filters out null results (partitions without snapshots).</description></item>
        /// <item><description>Returns a dictionary mapping partition IDs to their snapshots.</description></item>
        /// </list>
        /// <para>
        /// <strong>Concurrency:</strong> All snapshot retrievals execute concurrently. The actual parallelism
        /// depends on the ThreadPool and the underlying store's ability to handle concurrent requests.
        /// </para>
        /// <para>
        /// <strong>Error Handling:</strong> If an individual read fails, it is treated as if
        /// no snapshot exists for that partition (best-effort semantics). Only cancellation
        /// exceptions are propagated. Partial results are returned for successful retrievals.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="snapshotPartitionIds"/> is null.</exception>
        public async Task<IReadOnlyDictionary<string, SnapshotInfo>> GetManyAsync(
            IEnumerable<string> snapshotPartitionIds,
            CancellationToken cancellationToken)
        {
            if (snapshotPartitionIds == null)
                throw new ArgumentNullException(nameof(snapshotPartitionIds));

            // Deduplicate partition IDs to avoid redundant queries
            var distinctPartitionIds = snapshotPartitionIds.Distinct().ToList();

            // If no partition IDs provided, return an empty dictionary
            if (distinctPartitionIds.Count == 0)
                return new Dictionary<string, SnapshotInfo>();

            var results = new ConcurrentDictionary<string, SnapshotInfo>();
            var dop = Math.Min(Environment.ProcessorCount, distinctPartitionIds.Count);

            await ForEachAsync(dop, distinctPartitionIds, async (partitionId, ct) =>
            {
                try
                {
                    var snapshot = await _snapshotStore.GetLastAsync(partitionId, ct).ConfigureAwait(false);
                    if (snapshot != null)
                    {
                        results[partitionId] = snapshot;
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Best-effort: treat failure as "no snapshot found"
                    _logger.LogError($"GetLastAsync failed for partition {partitionId}. Exception: {ex.Message}.\n{ex}");
                }
            }, cancellationToken).ConfigureAwait(false);

            return results;
        }

        /// <summary>
        /// Queues multiple snapshots for background persistence using best-effort semantics.
        /// </summary>
        /// <param name="snapshots">Dictionary mapping partition IDs to their snapshot information.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// <para>
        /// <strong>Implementation Details:</strong>
        /// </para>
        /// <list type="number">
        /// <item><description>Filters out null or empty snapshots and enqueues them for background processing.</description></item>
        /// <item><description>The background worker prefers <see cref="ISnapshotStoreBatchWriter"/> when available.</description></item>
        /// <item><description>Otherwise executes <see cref="ISnapshotStore.AddAsync"/> for each snapshot in parallel.</description></item>
        /// <item><description>Ignores individual failures (best-effort semantics).</description></item>
        /// <item><description>Does not throw exceptions for snapshot save failures.</description></item>
        /// </list>
        /// <para>
        /// <strong>Best-Effort Rationale:</strong> Snapshot persistence is an optimization to avoid
        /// rebuilding state from events. If a snapshot fails to save, the system remains functional
        /// as it can rebuild from the event stream. This approach prioritizes system availability
        /// over snapshot consistency.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="snapshots"/> is null.</exception>
        public Task AddManyAsync(
            IReadOnlyDictionary<string, SnapshotInfo> snapshots,
            CancellationToken cancellationToken)
        {
            if (snapshots == null)
                throw new ArgumentNullException(nameof(snapshots));

            cancellationToken.ThrowIfCancellationRequested();

            var snapshotsToQueue = PrepareSnapshotsForQueue(snapshots);
            if (snapshotsToQueue.Count == 0)
                return Task.CompletedTask;

            lock (_lifecycleLock)
            {
                ThrowIfDisposedOrDisposing();
                _pendingSnapshotWrites.Enqueue(snapshotsToQueue);
            }

            _pendingSnapshotSignal.Release();
            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            lock (_lifecycleLock)
            {
                if (_isDisposed || _isDisposing)
                {
                    return;
                }

                _isDisposing = true;
            }

            _pendingSnapshotSignal.Release();

            try
            {
                await _queueProcessor.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error flushing snapshot queue during dispose. Exception: {ex.Message}.\n{ex}");
            }
            finally
            {
                lock (_lifecycleLock)
                {
                    _isDisposed = true;
                }

                _pendingSnapshotSignal.Dispose();
            }
        }

        private async Task ProcessQueueAsync()
        {
            while (true)
            {
                await _pendingSnapshotSignal.WaitAsync().ConfigureAwait(false);

                while (_pendingSnapshotWrites.TryDequeue(out var queuedSnapshots))
                {
                    try
                    {
                        await PersistSnapshotsAsync(queuedSnapshots, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        // Best-effort: snapshot writes are optimization-only.
                        _logger.LogError($"Snapshot queue worker failed to persist a batch. Exception: {ex.Message}.\n{ex}");
                    }
                }

                lock (_lifecycleLock)
                {
                    if (_isDisposing && _pendingSnapshotWrites.IsEmpty)
                    {
                        return;
                    }
                }
            }
        }

        private static IReadOnlyDictionary<string, SnapshotInfo> PrepareSnapshotsForQueue(
            IReadOnlyDictionary<string, SnapshotInfo> snapshots)
        {
            var validSnapshots = new Dictionary<string, SnapshotInfo>();
            foreach (var snapshot in snapshots)
            {
                if (snapshot.Value != null && !snapshot.Value.IsEmpty)
                {
                    validSnapshots[snapshot.Key] = snapshot.Value;
                }
            }

            return validSnapshots;
        }

        private async Task PersistSnapshotsAsync(
            IReadOnlyDictionary<string, SnapshotInfo> snapshots,
            CancellationToken cancellationToken)
        {
            if (snapshots.Count == 0)
            {
                return;
            }

            if (_snapshotStore is ISnapshotStoreBatchWriter batchWriter)
            {
                try
                {
                    await batchWriter.AddManyAsync(snapshots, cancellationToken).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Fallback to per-snapshot writes when optimized path fails.
                    _logger.LogError($"Batch AddManyAsync optimization failed. Falling back to per-snapshot writes. Exception: {ex.Message}.\n{ex}");
                }
            }

            var dop = Math.Min(Environment.ProcessorCount, snapshots.Count);

            await ForEachAsync(dop, snapshots, async (snapshot, ct) =>
            {
                try
                {
                    await _snapshotStore.AddAsync(snapshot.Key, snapshot.Value, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Best-effort: silently ignore failures
                    _logger.LogError($"AddAsync failed for partition {snapshot.Key}. Exception: {ex.Message}.\n{ex}");
                }
            }, cancellationToken).ConfigureAwait(false);
        }

        private void ThrowIfDisposedOrDisposing()
        {
            if (_isDisposed || _isDisposing)
            {
                throw new ObjectDisposedException(nameof(DefaultSnapshotBatchStore));
            }
        }

        private static Task ForEachAsync<T>(
            int maxDegreeOfParallelism,
            IEnumerable<T> source,
            Func<T, CancellationToken, Task> body,
            CancellationToken cancellationToken)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (body == null) throw new ArgumentNullException(nameof(body));
            if (maxDegreeOfParallelism <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));

            return ForEachAsyncCore(source, maxDegreeOfParallelism, body, cancellationToken);
        }

        private static async Task ForEachAsyncCore<T>(
            IEnumerable<T> source,
            int maxDegreeOfParallelism,
            Func<T, CancellationToken, Task> body,
            CancellationToken cancellationToken)
        {
            using var throttler = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();

            foreach (var item in source)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await body(item, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        throttler.Release();
                    }
                }, cancellationToken));
            }

            List<Exception> exceptions = null;

            while (tasks.Count > 0)
            {
                var completed = await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks.Remove(completed);

                try
                {
                    await completed.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    exceptions ??= new List<Exception>();
                    exceptions.Add(ex);
                }
            }

            if (exceptions != null)
            {
                throw new AggregateException(exceptions);
            }
        }
    }
}
