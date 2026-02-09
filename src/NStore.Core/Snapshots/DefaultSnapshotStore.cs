using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Snapshots
{
    public class DefaultSnapshotStore : ISnapshotStore, ISnapshotStoreBatchWriter
    {
        private readonly IPersistence _store;

        public DefaultSnapshotStore(IPersistence store)
        {
            _store = store;
        }

        public async Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
        {
            var data = await _store.ReadSingleBackwardAsync(snapshotPartitionId, long.MaxValue, cancellationToken).ConfigureAwait(false);
            return (SnapshotInfo)data?.Payload;
        }

        public async Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
        {
            var data = await _store.ReadSingleBackwardAsync(snapshotPartitionId, version, cancellationToken).ConfigureAwait(false);
            return (SnapshotInfo) data?.Payload;
        }

        public async Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
        {
            if (snapshot == null || snapshot.IsEmpty)
                return;

            try
            {
                await _store.AppendAsync(snapshotPartitionId, snapshot.SourceVersion, snapshot, null, cancellationToken).ConfigureAwait(false);
            }
            catch (DuplicateStreamIndexException)
            {
                // already stored
            }
        }

        public async Task AddManyAsync(
            IReadOnlyDictionary<string, SnapshotInfo> snapshots,
            CancellationToken cancellationToken)
        {
            if (snapshots == null)
                throw new ArgumentNullException(nameof(snapshots));

            var validSnapshots = snapshots
                .Where(kvp => kvp.Value != null && !kvp.Value.IsEmpty)
                .ToArray();

            if (validSnapshots.Length == 0)
                return;

            if (_store is IEnhancedPersistence enhancedPersistence)
            {
                var jobs = validSnapshots
                    .Select(kvp => new WriteJob(kvp.Key, kvp.Value.SourceVersion, kvp.Value, null))
                    .ToArray();

                await enhancedPersistence.AppendBatchAsync(jobs, cancellationToken).ConfigureAwait(false);
                return;
            }

            foreach (var kvp in validSnapshots)
            {
                await AddAsync(kvp.Key, kvp.Value, cancellationToken).ConfigureAwait(false);
            }
        }

        public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
        {
            return _store.DeleteAsync(snapshotPartitionId, fromVersionInclusive, toVersionInclusive, cancellationToken);
        }
    }
}
