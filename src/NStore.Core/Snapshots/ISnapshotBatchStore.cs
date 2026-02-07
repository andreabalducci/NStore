using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Snapshots
{
    /// <summary>
    /// Provides batch read and write operations for snapshots across multiple partitions.
    /// This interface enables efficient bulk operations when you need to load or store snapshots
    /// for many aggregates or entities in a single operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This interface is designed to complement <see cref="ISnapshotStore"/> by providing
    /// batch capabilities. Implementations may optimize batch operations at the storage level
    /// (e.g., using database batch queries) or execute individual operations in parallel.
    /// </para>
    /// <para>
    /// Typical use cases include:
    /// - Loading snapshots for multiple aggregates during query processing
    /// - Bulk snapshot retrieval for reporting or analysis
    /// - Preloading snapshots for a set of related entities
    /// - Batch saving snapshots after processing multiple aggregates
    /// </para>
    /// </remarks>
    public interface ISnapshotBatchStore : IAsyncDisposable
    {
        /// <summary>
        /// Retrieves the most recent snapshots for multiple partitions in a single operation.
        /// </summary>
        /// <param name="snapshotPartitionIds">Collection of partition IDs to retrieve snapshots for.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>
        /// A dictionary mapping partition IDs to their corresponding <see cref="SnapshotInfo"/>.
        /// Only partitions that have snapshots are included in the result. If a partition has no snapshot,
        /// it will not appear in the returned dictionary.
        /// </returns>
        /// <remarks>
        /// <para>
        /// <strong>Return Value Semantics:</strong> The returned dictionary only contains entries for
        /// partitions where a snapshot exists. If you request 10 partition IDs but only 7 have snapshots,
        /// the dictionary will contain 7 entries. Check for the presence of a key to determine if a
        /// partition has a snapshot.
        /// </para>
        /// <para>
        /// <strong>Performance:</strong> Implementations may execute reads in parallel or use batch
        /// queries to optimize performance. The order of results is not guaranteed.
        /// </para>
        /// <para>
        /// <strong>Example Usage:</strong>
        /// <code>
        /// var partitionIds = new[] { "Order-123", "Order-456", "Order-789" };
        /// var snapshots = await reader.GetManyAsync(partitionIds, cancellationToken);
        ///
        /// foreach (var partitionId in partitionIds)
        /// {
        ///     if (snapshots.TryGetValue(partitionId, out var snapshot))
        ///     {
        ///         // Snapshot exists for this partition
        ///         ProcessSnapshot(snapshot);
        ///     }
        ///     else
        ///     {
        ///         // No snapshot for this partition, need to rebuild from events
        ///         RebuildFromEvents(partitionId);
        ///     }
        /// }
        /// </code>
        /// </para>
        /// </remarks>
        Task<IReadOnlyDictionary<string, SnapshotInfo>> GetManyAsync(
            IEnumerable<string> snapshotPartitionIds,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Stores multiple snapshots in a single operation using best-effort semantics.
        /// </summary>
        /// <param name="snapshots">Dictionary mapping partition IDs to their snapshot information.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// <para>
        /// <strong>Best-Effort Semantics:</strong> This method attempts to store all provided snapshots
        /// but does not fail if individual snapshots cannot be saved. Failed snapshot saves are logged
        /// or silently ignored, as snapshot persistence is an optimization and not critical to system
        /// correctness. The system can always rebuild state from the event stream if a snapshot is missing.
        /// </para>
        /// <para>
        /// <strong>Deferred Execution:</strong> Implementations may buffer snapshot writes asynchronously
        /// for throughput reasons. Callers should dispose the batch store (via <see cref="IAsyncDisposable.DisposeAsync"/>)
        /// to flush any pending snapshot writes before shutdown.
        /// </para>
        /// <para>
        /// <strong>Performance:</strong> Implementations may execute writes in parallel or use batch
        /// queries to optimize performance. The order of writes is not guaranteed.
        /// </para>
        /// <para>
        /// <strong>Example Usage:</strong>
        /// <code>
        /// var snapshots = new Dictionary&lt;string, SnapshotInfo&gt;
        /// {
        ///     { "Order-123", new SnapshotInfo("Order-123", 5, orderState, "v1") },
        ///     { "Order-456", new SnapshotInfo("Order-456", 10, orderState2, "v1") }
        /// };
        /// await batchStore.AddManyAsync(snapshots, cancellationToken);
        /// </code>
        /// </para>
        /// </remarks>
        Task AddManyAsync(
            IReadOnlyDictionary<string, SnapshotInfo> snapshots,
            CancellationToken cancellationToken
        );
    }
}
