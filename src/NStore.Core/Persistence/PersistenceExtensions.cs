using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Core.Persistence;

/// <summary>
/// Extension functions that allow for a simpler API than using the
/// basic <see cref="IPersistence.ReadForwardAsync"/> base function.
/// </summary>
public static class PersistenceExtensions
{
    public static async Task AppendBatchAsync(
        this IEnhancedPersistence persistence,
        WriteJob[] queue,
        ParallelBatchAppendOptions options,
        CancellationToken cancellationToken = default)
    {
        if (persistence == null)
        {
            throw new ArgumentNullException(nameof(persistence));
        }

        if (queue == null)
        {
            throw new ArgumentNullException(nameof(queue));
        }

        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BatchSize), "BatchSize must be greater than zero.");
        }

        if (options.MaxWriters <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.MaxWriters), "MaxWriters must be greater than zero.");
        }

        if (queue.Length == 0)
        {
            return;
        }

        var batches = SplitInBatches(queue, options.BatchSize);

#if NET6_0_OR_GREATER
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = options.MaxWriters,
            CancellationToken = cancellationToken
        };

        await Parallel.ForEachAsync(
                batches,
                parallelOptions,
                async (batch, ct) => { await persistence.AppendBatchAsync(batch, ct).ConfigureAwait(false); })
            .ConfigureAwait(false);
#else
        await NStore.Core.AsyncParallelExtensions.ForEachAsync(
                batches,
                options.MaxWriters,
                (batch, ct) => persistence.AppendBatchAsync(batch, ct),
                cancellationToken)
            .ConfigureAwait(false);
#endif
    }

    private static List<WriteJob[]> SplitInBatches(WriteJob[] queue, int batchSize)
    {
        var batchCount = (queue.Length + batchSize - 1) / batchSize;
        var batches = new List<WriteJob[]>(batchCount);

        for (var offset = 0; offset < queue.Length; offset += batchSize)
        {
            var currentBatchSize = Math.Min(batchSize, queue.Length - offset);
            var batch = new WriteJob[currentBatchSize];
            Array.Copy(queue, offset, batch, 0, currentBatchSize);
            batches.Add(batch);
        }

        return batches;
    }

    public static Task ReadForwardAsync(
        this IPersistence persistence,
        string partitionId,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardAsync(
            partitionId,
            0,
            subscription,
            long.MaxValue,
            int.MaxValue,
            cancellationToken
        );
    }

    public static Task ReadForwardMultiplePartitionsAsync(
        this IMultiPartitionPersistenceReader persistence,
        IEnumerable<string> partitionIdsList,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardMultiplePartitionsAsync(
            partitionIdsList: partitionIdsList,
            fromLowerIndexInclusive: 0,
            subscription: subscription,
            toUpperIndexInclusive: long.MaxValue,
            cancellationToken: cancellationToken
        );
    }

    public static Task ReadForwardAsync(
        this IPersistence persistence,
        string partitionId,
        long fromLowerIndexInclusive,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardAsync(
            partitionId,
            fromLowerIndexInclusive,
            subscription,
            long.MaxValue,
            int.MaxValue,
            cancellationToken
        );
    }

    public static Task ReadForwardMultiplePartitionsAsync(
        this IMultiPartitionPersistenceReader persistence,
        IEnumerable<string> partitionIdsList,
        long fromLowerIndexInclusive,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardMultiplePartitionsAsync(
            partitionIdsList: partitionIdsList,
            fromLowerIndexInclusive: fromLowerIndexInclusive,
            subscription: subscription,
            toUpperIndexInclusive: long.MaxValue,
            cancellationToken: cancellationToken
        );
    }

    public static Task ReadForwardAsync(
        this IPersistence persistence,
        string partitionId,
        long fromLowerIndexInclusive,
        ISubscription subscription,
        long toUpperIndexInclusive,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardAsync(
            partitionId,
            fromLowerIndexInclusive,
            subscription,
            toUpperIndexInclusive,
            int.MaxValue,
            cancellationToken
        );
    }

    public static Task ReadForwardMultiplePartitionsAsync(
        this IMultiPartitionPersistenceReader persistence,
        IEnumerable<string> partitionIdsList,
        long fromLowerIndexInclusive,
        ISubscription subscription,
        long toUpperIndexInclusive,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardMultiplePartitionsAsync(
            partitionIdsList,
            fromLowerIndexInclusive: fromLowerIndexInclusive,
            subscription: subscription,
            toUpperIndexInclusive: toUpperIndexInclusive,
            cancellationToken: cancellationToken
        );
    }

#if NET8_0_OR_GREATER
    public static IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
        this IMultiPartitionPersistenceReader persistence,
        IEnumerable<string> partitionIdsList,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardMultiplePartitionsAsyncEnumerable(
            partitionIdsList: partitionIdsList,
            fromLowerIndexInclusive: 0,
            toUpperIndexInclusive: long.MaxValue,
            cancellationToken: cancellationToken
        );
    }

    public static IAsyncEnumerable<IChunk> ReadForwardMultiplePartitionsAsyncEnumerable(
        this IMultiPartitionPersistenceReader persistence,
        IEnumerable<string> partitionIdsList,
        long fromLowerIndexInclusive,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadForwardMultiplePartitionsAsyncEnumerable(
            partitionIdsList: partitionIdsList,
            fromLowerIndexInclusive: fromLowerIndexInclusive,
            toUpperIndexInclusive: long.MaxValue,
            cancellationToken: cancellationToken
        );
    }
#endif

    public static Task ReadBackwardAsync(
        this IPersistence persistence,
        string partitionId,
        long fromUpperIndexInclusive,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadBackwardAsync(
            partitionId,
            fromUpperIndexInclusive,
            subscription,
            0,
            int.MaxValue,
            cancellationToken
        );
    }

    public static Task ReadBackwardAsync(
        this IPersistence persistence,
        string partitionId,
        long fromUpperIndexInclusive,
        ISubscription subscription,
        long toLowerIndexInclusive,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadBackwardAsync(
            partitionId,
            fromUpperIndexInclusive,
            subscription,
            toLowerIndexInclusive,
            int.MaxValue,
            cancellationToken
        );
    }

    public static Task ReadAllAsync(
        this IPersistence persistence,
        long fromPositionInclusive,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadAllAsync(fromPositionInclusive, subscription, int.MaxValue, cancellationToken);
    }

    public static Task ReadAllAsync(
        this IPersistence persistence,
        long fromPositionInclusive,
        ISubscription subscription,
        int limit,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadAllAsync(fromPositionInclusive, subscription, limit, cancellationToken);
    }

    public static Task<IChunk> ReadSingleBackwardAsync(
        this IPersistence persistence,
        string partitionId,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadSingleBackwardAsync(partitionId, long.MaxValue, cancellationToken);
    }

    public static Task<IChunk> AppendAsync(
        this IPersistence persistence,
        string partitionId,
        long index,
        object payload,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.AppendAsync(partitionId, index, payload, null, cancellationToken);
    }

    public static Task<IChunk> AppendAsync(
        this IPersistence persistence,
        string partitionId,
        long index,
        object payload,
        string operationId,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.AppendAsync(partitionId, index, payload, operationId, cancellationToken);
    }

    public static Task DeleteAsync(
        this IPersistence persistence,
        string partitionId,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.DeleteAsync(partitionId, 0, long.MaxValue, cancellationToken);
    }

    public static Task DeleteAsync(
        this IPersistence persistence,
        string partitionId,
        long fromLowerIndexInclusive,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, long.MaxValue, cancellationToken);
    }

    public static Task DeleteAsync(
        this IPersistence persistence,
        string partitionId,
        long fromLowerIndexInclusive,
        long toUpperIndexInclusive,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.DeleteAsync(partitionId, fromLowerIndexInclusive, toUpperIndexInclusive, cancellationToken);
    }

    public static Task<long> ReadLastPositionAsync(
        this IPersistence persistence,
        CancellationToken cancellationToken = default)
    {
        return persistence.ReadLastPositionAsync(cancellationToken);
    }

    public static Task<IChunk> ReadByOpeationIdAsync(
        this IPersistence persistence,
        string partitionId,
        string operationId,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadByOperationIdAsync(partitionId, operationId, cancellationToken);
    }

    public static Task ReadAllByOperationIdAsync(
        this IPersistence persistence,
        string operationId,
        ISubscription subscription,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadAllByOperationIdAsync(operationId, subscription, cancellationToken);
    }

    public static Task<IChunk> ReplaceOneAsync(
        this IPersistence persistence,
        long position,
        string partitionId,
        long index,
        object payload,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReplaceOneAsync(position, partitionId, index, payload, null, cancellationToken);
    }

    public static Task<IChunk> ReadOneAsync(
        this IPersistence persistence,
        long position,
        CancellationToken cancellationToken = default
    )
    {
        return persistence.ReadOneAsync(position, cancellationToken);
    }
}
