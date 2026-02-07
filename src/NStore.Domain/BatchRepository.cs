using NStore.Core.Persistence;
using NStore.Core.Snapshots;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Domain
{
    /// <summary>
    /// Batch repository implementation for efficient multi-aggregate operations.
    /// Uses ISnapshotBatchStore, IMultiPartitionPersistenceReader, and IEnhancedPersistence
    /// to perform batch reads and writes with optimistic concurrency control.
    /// </summary>
    public class BatchRepository : IBatchRepository
    {
        private readonly IAggregateFactory _factory;
        private readonly IEnhancedPersistence _persistence;
        private readonly ISnapshotBatchStore _snapshotBatchStore;

        private readonly ConcurrentDictionary<string, IAggregate> _trackingAggregates = new ConcurrentDictionary<string, IAggregate>();

        public bool PersistEmptyChangeset { get; set; } = false;

        public BatchRepository(
            IAggregateFactory factory,
            IEnhancedPersistence persistence,
            ISnapshotBatchStore snapshotStore)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
            _snapshotBatchStore = snapshotStore;
        }

        public async Task<IReadOnlyDictionary<string, T>> GetManyByIdAsync<T>(
            IReadOnlyCollection<string> ids,
            CancellationToken cancellationToken = default) where T : IAggregate, IEventSourcedAggregate
        {
            if (ids.Count == 0)
            {
#if NET8_0_OR_GREATER
                return System.Collections.ObjectModel.ReadOnlyDictionary<string, T>.Empty;
#else
                return new ReadOnlyDictionary<string, T>(new Dictionary<string, T>());
#endif
            }

            var aggregates = new Dictionary<string, T>();

            // Step 1: Check tracking cache first
            List<string> idsToLoad = GetListOfIdToLoad(ids, aggregates);

            // all id were already loaded. we can exit.
            if (idsToLoad.Count == 0)
            {
                return aggregates;
            }

            // Step 2: Create aggregates to be loaded with event sourcing.
            CreateAggregates(aggregates, idsToLoad);

            // Step 3: Load snapshots if available
            await LoadSnapshots(aggregates, idsToLoad, cancellationToken).ConfigureAwait(false);

            // Step 3b: Initialize aggregates that weren't restored from snapshots
            foreach (var notInitializedAggregateEntry in aggregates.Where(a => !a.Value.IsInitialized))
            {
                notInitializedAggregateEntry.Value.Init(notInitializedAggregateEntry.Key);
            }

            // Step 4: Build partition read requests based on aggregate versions
            var partitionRequests = new List<PartitionReadRequest>();
            var snapshotVersions = new Dictionary<string, long>();
            LoadPartitionRequest(aggregates, partitionRequests, snapshotVersions);

            // Step 5: Read events from multiple partitions, it is imperative that we do not store
            //         chunks in memory or we will use too much memory for large batches.
            var countOfEventsRead = new Dictionary<string, int>();
            var subscription = new LambdaSubscription(chunk =>
            {
                //we immediately restore the aggregate
                var partitionId = chunk.PartitionId;
                if (aggregates.TryGetValue(partitionId, out var aggregate))
                {
                    aggregate.ApplyChanges((Changeset)chunk.Payload);
                    if (!countOfEventsRead.ContainsKey(partitionId))
                    {
                        countOfEventsRead[partitionId] = 0;
                    }
                    countOfEventsRead[partitionId] += 1;
                }

                // the else should never happen, but just in case, we ignore chunks for unknown partitions
                return Task.FromResult(true);
            });

            await _persistence.ReadForwardMultiplePartitionsWithRangesAsync(
                partitionRequests,
                subscription,
                cancellationToken
            ).ConfigureAwait(false);

            // Check for subscription errors
            if (subscription.Failed)
            {
                var aggregateIds = string.Join(", ", aggregates.Keys);
                throw new RepositoryReadException(
                    $"Error reading aggregates in batch. Aggregate IDs: [{aggregateIds}]",
                    subscription.LastError);
            }

            // Mark aggregates as loaded and check for stale snapshots
            foreach (var kvp in aggregates)
            {
                kvp.Value.Loaded();

                var snapshotVersion = snapshotVersions[kvp.Key];
                countOfEventsRead.TryGetValue(kvp.Key, out var eventsRead);

                if (snapshotVersion > 0 && eventsRead == 0)
                    throw new StaleSnapshotException(kvp.Value.Id, snapshotVersion);
            }

            return aggregates;
        }

        private static void LoadPartitionRequest<T>(Dictionary<string, T> aggregates, List<PartitionReadRequest> partitionRequests, Dictionary<string, long> snapshotVersions) where T : IAggregate
        {
            foreach (var kvp in aggregates)
            {
                // Track snapshot version for stale snapshot detection
                snapshotVersions[kvp.Key] = kvp.Value.Version;

                // Read from current version to end
                // Note: Starting point is inclusive, so we read from aggregate.Version
                // The aggregate will ignore duplicate events because ApplyChanges is idempotent
                partitionRequests.Add(new PartitionReadRequest(
                    kvp.Key,
                    kvp.Value.Version,
                    long.MaxValue
                ));
            }
        }

        private async Task LoadSnapshots<T>(Dictionary<string, T> aggregates, List<string> idsToLoad, CancellationToken cancellationToken) where T : IAggregate
        {
            // Usually all our aggregate are snapshottable so we can try to load snapshots in batch. Also, when we load
            // batch snapshot, if an aggregate is not snapshottable, we will have no snapshot, so there is no need to check
            // if the aggregate is snapshottable before loading snapshots.
            if (_snapshotBatchStore != null)
            {
                var snapshots = await _snapshotBatchStore.GetManyAsync(idsToLoad, cancellationToken).ConfigureAwait(false);

                // In first implementation we have a parallel foreach, but restoring snapshots is very fast operation
                // we prefer a cleaner code
                foreach (var snapshotKey in snapshots)
                {
                    if (aggregates.TryGetValue(snapshotKey.Key, out var aggregate) && aggregate is ISnapshottable snapshottable)
                    {
                        if (!snapshottable.TryRestore(snapshotKey.Value))
                        {
                            // Snapshot restoration failed (e.g., type mismatch).
                            // Recreate aggregate fresh - it will be initialized later and load all events.
                            var freshAggregate = _factory.Create<T>();
                            aggregates[snapshotKey.Key] = freshAggregate;
                            _trackingAggregates[snapshotKey.Key] = freshAggregate;
                        }
                    }
                }
            }
        }

        private void CreateAggregates<T>(Dictionary<string, T> aggregates, List<string> idsToLoad) where T : IAggregate
        {
            foreach (var id in idsToLoad)
            {
                var aggregate = _factory.Create<T>();
                aggregates[id] = aggregate;
                _trackingAggregates[id] = aggregate;
            }
        }

        private List<string> GetListOfIdToLoad<T>(IEnumerable<string> ids, Dictionary<string, T> aggregates) where T : IAggregate
        {
            var idsToLoad = new List<string>();
            foreach (var id in ids)
            {
                if (_trackingAggregates.TryGetValue(id, out var cachedAggregate))
                {
                    aggregates[id] = (T)cachedAggregate;
                }
                else
                {
                    idsToLoad.Add(id);
                }
            }

            return idsToLoad;
        }

        /// <summary>
        /// Saves a collection of aggregates in a single batch.
        /// Note: when a concurrency conflict or other write failure occurs for an aggregate,
        /// the repository automatically removes that aggregate from its internal tracking cache
        /// to avoid leaving stale, uncommitted state. Callers that wish to retry a failed aggregate
        /// can either call <see cref="Clear()"/> and reload the aggregate or simply call
        /// <see cref="GetManyByIdAsync{T}(IReadOnlyCollection{string}, CancellationToken)"/>, which
        /// will return a fresh instance if the previous one was removed due to failure.
        /// </summary>
        public async Task<BatchSaveResult> SaveManyAsync(
            IReadOnlyList<IAggregate> aggregates,
            string operationId,
            Action<IHeadersAccessor> headers = null,
            CancellationToken cancellationToken = default,
            ParallelBatchAppendOptions parallelBatchAppendOptions = null)
        {
            if (!aggregates.Any())
            {
                return BatchSaveResult.Empty;
            }

            // Step 1: Validate all aggregates and prepare write jobs
            var writeJobs = new List<WriteJob>();
            var aggregateByPartitionId = new Dictionary<string, IAggregate>();
            var listOfAggregateSaveResult = new List<AggregateSaveResult>();
            PrepareAggregates(aggregates, operationId, headers, writeJobs, aggregateByPartitionId, listOfAggregateSaveResult);

            if (writeJobs.Count > 0)
            {
                var writeJobsArray = writeJobs.ToArray();

                // Step 2: Execute batch append
                await AppendWriteJobsAsync(writeJobsArray, parallelBatchAppendOptions, cancellationToken).ConfigureAwait(false);
                var snapshotsToSave = new Dictionary<string, SnapshotInfo>();

                foreach (var job in writeJobsArray)
                {
                    var aggregate = aggregateByPartitionId[job.PartitionId];
                    listOfAggregateSaveResult.Add(MapJobResult(job, aggregate.Id));

                    if (job.Result == WriteJob.WriteResult.Committed)
                    {
                        ((IEventSourcedAggregate)aggregate).Persisted(((IEventSourcedAggregate)aggregate).GetChangeSet());
                        if (_snapshotBatchStore != null && aggregate is ISnapshottable snapshottable)
                        {
                            snapshotsToSave[aggregate.Id] = snapshottable.GetSnapshot();
                        }
                    }
                    else if (job.Result == WriteJob.WriteResult.DuplicatedIndex || 
                             job.Result == WriteJob.WriteResult.Failed || 
                             job.Result == WriteJob.WriteResult.None)
                    {
                        // On concurrency (DuplicatedIndex) or other write failures we remove the aggregate
                        // from the tracking cache so subsequent operations will reload the aggregate from
                        // persistence instead of reusing a stale in-memory instance (see tests).
                        _trackingAggregates.TryRemove(aggregate.Id, out _);
                    }
                }

                // Step 4: Save snapshots in batch (best-effort)
                if (_snapshotBatchStore != null && snapshotsToSave.Count > 0)
                {
                    await _snapshotBatchStore.AddManyAsync(snapshotsToSave, cancellationToken).ConfigureAwait(false);
                }
            }

            // Step 5: Return batch save result with all aggregate results
            return new BatchSaveResult
            {
                Results = listOfAggregateSaveResult
            };
        }

        private Task AppendWriteJobsAsync(
            WriteJob[] writeJobs,
            ParallelBatchAppendOptions options,
            CancellationToken cancellationToken)
        {
            if (options == null)
            {
                return _persistence.AppendBatchAsync(writeJobs, cancellationToken);
            }

            if (options.BatchSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.BatchSize), "BatchSize must be greater than zero.");
            }

            if (options.MaxWriters <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.MaxWriters), "MaxWriters must be greater than zero.");
            }

            var shouldUseParallelAppend = options.MaxWriters > 1 && writeJobs.Length > options.BatchSize;
            return shouldUseParallelAppend
                ? _persistence.AppendBatchAsync(writeJobs, options, cancellationToken)
                : _persistence.AppendBatchAsync(writeJobs, cancellationToken);
        }

        private void PrepareAggregates(IEnumerable<IAggregate> aggregates, string operationId, Action<IHeadersAccessor> headers, List<WriteJob> writeJobs, Dictionary<string, IAggregate> aggregateByPartitionId, List<AggregateSaveResult> listOfAggregateSaveResult)
        {
            var seen = new HashSet<string>();
            foreach (var aggregate in aggregates)
            {
                var result = PrepareAggregate(aggregate, operationId, headers, seen);

                if (result.EarlyResult != null)
                {
                    listOfAggregateSaveResult.Add(result.EarlyResult);
                    continue;
                }

                writeJobs.Add(result.Job);
                aggregateByPartitionId[aggregate.Id] = aggregate;
            }
        }

        /// <summary>
        /// Clears the repository's tracking cache of aggregates.
        /// Use this to discard any in-memory aggregate instances and force reloads from persistence.
        /// </summary>
        public void Clear()
        {
            _trackingAggregates.Clear();
        }

        private (AggregateSaveResult EarlyResult, WriteJob Job) PrepareAggregate(
            IAggregate aggregate,
            string operationId,
            Action<IHeadersAccessor> headers,
            HashSet<string> seen)
        {
            // Check for duplicate aggregate ids in the input
            if (!seen.Add(aggregate.Id))
                throw new ArgumentException($"Duplicate aggregate id '{aggregate.Id}' passed to SaveManyAsync");

            // Validate aggregate is tracked by this repository (unless it's a new aggregate)
            if (!_trackingAggregates.ContainsKey(aggregate.Id))
            {
                if (aggregate.IsNew)
                    _trackingAggregates[aggregate.Id] = aggregate;
                else
                    throw new RepositoryMismatchException($"Aggregate {aggregate.Id} was not loaded by this batch repository");
            }

            var persister = (IEventSourcedAggregate)aggregate;
            var changeSet = persister.GetChangeSet();

            // Skip empty changesets unless configured to persist them
            if (changeSet.IsEmpty() && !PersistEmptyChangeset)
                return (AggregateSaveResult.Unchanged(aggregate.Id), null);

            // Check invariants if supported
            if (aggregate is IInvariantsChecker checker && checker.CheckInvariants().IsInvalid)
                return (AggregateSaveResult.InvariantFailure(aggregate.Id), null);

            // Apply headers if provided
            headers?.Invoke(changeSet);

            var job = new WriteJob(
                aggregate.Id,
                changeSet.AggregateVersion,
                changeSet,
                operationId ?? Guid.NewGuid().ToString()
            );

            return (null, job);
        }

        private static AggregateSaveResult MapJobResult(WriteJob job, string aggregateId)
        {
            switch (job.Result)
            {
                case WriteJob.WriteResult.Committed: return AggregateSaveResult.Committed(aggregateId, job.Chunk);
                case WriteJob.WriteResult.DuplicatedIndex: return AggregateSaveResult.Concurrency(aggregateId);
                case WriteJob.WriteResult.DuplicatedOperation: return AggregateSaveResult.DuplicatedOperation(aggregateId, job.Chunk);
                case WriteJob.WriteResult.DuplicatedPosition: return AggregateSaveResult.DuplicatedPosition(aggregateId);

                case WriteJob.WriteResult.None:
                case WriteJob.WriteResult.Failed:
                default: return AggregateSaveResult.GenericFailure(aggregateId);
            }
        }
    }
}
