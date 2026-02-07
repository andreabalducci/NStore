using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Snapshots
{
    /// <summary>
    /// Optional capability interface for snapshot stores that can persist many snapshots
    /// in a single optimized backend operation.
    /// </summary>
    /// <remarks>
    /// Implementations should follow best-effort semantics: store as many snapshots as possible
    /// and avoid throwing for per-item write conflicts. Cancellation should still be propagated.
    /// </remarks>
    public interface ISnapshotStoreBatchWriter
    {
        Task AddManyAsync(
            IReadOnlyDictionary<string, SnapshotInfo> snapshots,
            CancellationToken cancellationToken
        );
    }
}
