using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Domain
{
    /// <summary>
    /// Batch repository for efficient multi-aggregate operations.
    /// Enables loading and saving multiple aggregates in a single batch operation.
    /// </summary>
    public interface IBatchRepository
    {
        /// <summary>
        /// Loads multiple aggregates of the same type by their IDs.
        /// Uses batch snapshot reading and multi-partition event reading for optimal performance.
        /// </summary>
        /// <typeparam name="T">The aggregate type</typeparam>
        /// <param name="ids">Collection of aggregate IDs to load</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Dictionary mapping aggregate IDs to loaded aggregates. Only successfully loaded aggregates are included.</returns>
        Task<IReadOnlyDictionary<string, T>> GetManyByIdAsync<T>(
            IReadOnlyCollection<string> ids,
            CancellationToken cancellationToken = default
        ) where T : IAggregate, IEventSourcedAggregate;

        /// <summary>
        /// Saves multiple aggregates in a single batch operation and returns a `BatchSaveResult` describing per-aggregate outcomes.
        /// Does NOT throw exception for concurrency conflicts; check the returned result.
        /// Automatically retries on position conflicts (duplicate global IDs).
        /// 
        /// <para>
        /// <strong>Important tracking behavior:</strong> If an aggregate fails to save (e.g. concurrency conflict), 
        /// it is automatically <strong>removed</strong> from the repository's internal tracking cache.
        /// This prevents stale state from being reused. 
        /// </para>
        /// <para>
        /// <strong>To retry:</strong> You should call <see cref="Clear"/> and reload the aggregates, 
        /// or instantiate a new repository instance. Do not try to reuse the same failed aggregate instances.
        /// </para>
        /// </summary>
        /// <param name="aggregates">Collection of aggregates to save</param>
        /// <param name="operationId">Operation ID for idempotency, it can be null, where using null means
        /// that the repository will generate unique operation id. Usually I'm expecting this value to be passed
        /// from the external code, because probably all the update of all the aggregate will be written on the
        /// very same operation id for a full idempotency: e.g., the caller calls an API, a transport error arises, he/she can
        /// request the same batch of operation with the same id for full idempotency.</param>
        /// <param name="headers">Optional action to add headers to changesets</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="parallelBatchAppendOptions">
        /// Optional options to enable split/parallel append for this save call only.
        /// When null, SaveManyAsync uses a single AppendBatchAsync call.
        /// </param>
        Task<BatchSaveResult> SaveManyAsync(
            IReadOnlyList<IAggregate> aggregates,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default,
            ParallelBatchAppendOptions parallelBatchAppendOptions = null
        );

        /// <summary>
        /// Clears all internal tracking state.
        /// Useful after concurrency exceptions to reload aggregates for retry.
        /// </summary>
        void Clear();
    }
}
