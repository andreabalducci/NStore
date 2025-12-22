# NStore Batch Operations Plan

> **Created**: 2024-12-22
> **Target**: .NET 10 / C# 13
> **Scope**: Multi-stream batch operations with reduced roundtrips

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Use Cases](#use-cases)
3. [Current Architecture Analysis](#current-architecture-analysis)
4. [Proposed Architecture](#proposed-architecture)
5. [Implementation Plan](#implementation-plan)
6. [API Design](#api-design)
7. [Performance Optimizations](#performance-optimizations)
8. [Error Handling](#error-handling)
9. [Testing Strategy](#testing-strategy)

---

## Executive Summary

### Problem Statement

Currently, updating multiple streams (e.g., "mark all orders as completed") requires:
- N database roundtrips (one per stream)
- Sequential processing or manual parallelization
- Difficult error recovery in partial failures

### Proposed Solution

Implement a batch operations framework that:
- Reduces database roundtrips through bulk operations
- Provides parallel processing with controlled concurrency
- Handles partial failures gracefully
- Maintains optimistic concurrency per stream

### Expected Improvements

| Operation | Current | Proposed | Improvement |
|-----------|---------|----------|-------------|
| Update 100 streams | 100 roundtrips | 1-10 roundtrips | 90-99% reduction |
| Parallel processing | Manual | Built-in | Developer experience |
| Error recovery | Manual | Automatic retry | Reliability |

---

## Use Cases

### UC-1: Batch Status Update
```csharp
// Mark all pending orders as completed
await repository.BatchUpdateAsync<Order>(
    orderIds,
    order => order.MarkAsCompleted(),
    operationId: "bulk-complete-2024-12-22"
);
```

### UC-2: Conditional Batch Operations
```csharp
// Cancel all reservations older than 30 minutes
var expiredIds = await FindExpiredReservations();
await repository.BatchUpdateAsync<Reservation>(
    expiredIds,
    reservation => reservation.Cancel("Expired"),
    predicate: r => r.Status == ReservationStatus.Pending
);
```

### UC-3: Parallel Stream Creation
```csharp
// Create 1000 orders from import file
var commands = LoadOrdersFromFile();
await repository.BatchCreateAsync<Order>(
    commands,
    maxParallelism: 10
);
```

---

## Current Architecture Analysis

### Existing Batch Support

**MongoDB** (`NStore.Persistence.Mongo`):
- ✅ Has `AppendBatchAsync` for bulk writes
- ✅ Uses `InsertManyAsync` with `IsOrdered = false`
- ❌ No batch read support

**SQL Server** (`NStore.Persistence.MsSql`):
- ❌ No batch operations
- ⚠️ Could use table-valued parameters

**TPL Batch Decorator** (`NStore.Tpl`):
- ✅ Batches individual appends
- ❌ Only works at persistence layer, not repository
- ❌ No multi-stream coordination

### Limitations

1. **No Repository-Level Batching**: Current `Repository` works one aggregate at a time
2. **Manual Parallelization**: Developers must use `Parallel.ForEachAsync` manually
3. **Limited Error Handling**: No built-in retry logic
4. **No Batch Reads**: Must read streams individually

---

## Proposed Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────┐
│                 Application Layer                    │
│  repository.BatchUpdateAsync<Order>(ids, action)     │
└─────────────────┬───────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────┐
│              IBatchRepository                        │
│  - BatchReadAsync<T>()                              │
│  - BatchUpdateAsync<T>()                            │
│  - BatchCreateAsync<T>()                            │
└─────────────────┬───────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────┐
│          BatchOperationCoordinator                   │
│  - Parallel execution with semaphore                │
│  - Retry logic with exponential backoff             │
│  - Progress tracking and cancellation               │
│  - Partial failure handling                         │
└─────────────────┬───────────────────────────────────┘
                  │
         ┌────────┴────────┐
         │                 │
┌────────▼─────┐  ┌───────▼────────┐
│ Repository   │  │ IPersistence   │
│ (per stream) │  │ (bulk ops)     │
└──────────────┘  └────────────────┘
```

### Key Components

1. **IBatchRepository**: High-level batch API for domain operations
2. **BatchOperationCoordinator**: Orchestrates parallel execution and error handling
3. **IEnhancedPersistence**: Extended with batch read/update capabilities
4. **BatchResult<T>**: Result type with success/failure details per item

### Batch Operation Rules

**Critical Constraints:**

1. **Shared OperationId**: All streams in a single batch operation share the same `operationId`
   - This enables idempotency across the entire batch
   - Retry of a failed batch uses the same `operationId` for all streams
   - Example: `"batch-complete-orders-2024-12-22-abc123"`

2. **Single Operation Per Stream**: A batch can contain at most ONE operation per stream
   - Each stream ID can appear only once in a batch
   - Multiple operations on the same stream must be in separate batches
   - This prevents conflicting concurrent updates to the same stream

3. **Roundtrip Optimization**: The goal is to minimize database roundtrips
   - Reading multiple streams: 1 query instead of N queries
   - Writing multiple streams: Parallel execution with bulk operations where possible
   - Expected: N roundtrips → 1-10 roundtrips (90-99% reduction)

**Example - Valid Batch:**
```csharp
await repository.BatchUpdateAsync<Order>(
    ["order-1", "order-2", "order-3"],  // Each stream appears once
    order => order.MarkAsCompleted(),
    operationId: "batch-complete-2024-12-22"  // Shared operationId
);
```

**Example - Invalid Batch:**
```csharp
// ❌ INVALID: order-1 appears twice
var operations = [
    ("order-1", (Order o) => o.AddItem(...)),
    ("order-1", (Order o) => o.UpdateStatus(...))  // ERROR: Duplicate stream
];
```

---

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)

#### 1.1 Define Batch Interfaces

```csharp
namespace NStore.Domain.Batch;

/// <summary>
/// Repository extensions for batch operations using .NET 10 features
/// </summary>
public interface IBatchRepository : IRepository
{
    /// <summary>
    /// Read multiple aggregates in parallel with reduced roundtrips
    /// </summary>
    IAsyncEnumerable<BatchReadResult<T>> BatchReadAsync<T>(
        IEnumerable<string> aggregateIds,
        BatchReadOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IAggregate;

    /// <summary>
    /// Update multiple aggregates with an action
    /// </summary>
    Task<BatchResult<T>> BatchUpdateAsync<T>(
        IEnumerable<string> aggregateIds,
        Action<T> updateAction,
        string operationId,
        BatchUpdateOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IAggregate;

    /// <summary>
    /// Create multiple new aggregates
    /// </summary>
    Task<BatchResult<T>> BatchCreateAsync<T>(
        IEnumerable<(string Id, Action<T> InitAction)> items,
        string operationId,
        BatchCreateOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IAggregate;
}

/// <summary>
/// Options for batch read operations
/// </summary>
public sealed record BatchReadOptions
{
    public int MaxParallelism { get; init; } = 10;
    public bool IncludeNotFound { get; init; } = false;
    public TimeSpan? Timeout { get; init; }
}

/// <summary>
/// Options for batch update operations
/// </summary>
public sealed record BatchUpdateOptions
{
    public int MaxParallelism { get; init; } = 10;
    public int MaxRetries { get; init; } = 3;
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public bool StopOnFirstError { get; init; } = false;
    public bool UseOptimisticConcurrency { get; init; } = true;
    public Predicate<IAggregate>? Predicate { get; init; }
}

/// <summary>
/// Options for batch create operations
/// </summary>
public sealed record BatchCreateOptions
{
    public int MaxParallelism { get; init; } = 10;
    public bool ContinueOnError { get; init; } = true;
}
```

#### 1.2 Result Types

```csharp
namespace NStore.Domain.Batch;

/// <summary>
/// Result of a batch operation with detailed success/failure tracking
/// </summary>
public sealed class BatchResult<T> where T : IAggregate
{
    public required int TotalCount { get; init; }
    public required int SuccessCount { get; init; }
    public required int FailureCount { get; init; }
    public required int SkippedCount { get; init; }

    public required IReadOnlyList<BatchItemResult<T>> Results { get; init; }
    public required TimeSpan Duration { get; init; }

    public bool IsSuccess => FailureCount == 0;
    public bool IsPartialSuccess => SuccessCount > 0 && FailureCount > 0;

    public IEnumerable<BatchItemResult<T>> Successes =>
        Results.Where(r => r.Status == BatchItemStatus.Success);

    public IEnumerable<BatchItemResult<T>> Failures =>
        Results.Where(r => r.Status == BatchItemStatus.Failed);

    public IEnumerable<BatchItemResult<T>> Skipped =>
        Results.Where(r => r.Status == BatchItemStatus.Skipped);
}

/// <summary>
/// Result for a single item in a batch operation
/// </summary>
public sealed record BatchItemResult<T> where T : IAggregate
{
    public required string AggregateId { get; init; }
    public required BatchItemStatus Status { get; init; }
    public T? Aggregate { get; init; }
    public Exception? Error { get; init; }
    public int RetryCount { get; init; }
}

public enum BatchItemStatus
{
    Success,
    Failed,
    Skipped,
    NotFound
}

/// <summary>
/// Result for batch read (streaming)
/// </summary>
public sealed record BatchReadResult<T> where T : IAggregate
{
    public required string AggregateId { get; init; }
    public T? Aggregate { get; init; }
    public Exception? Error { get; init; }
    public bool IsSuccess => Error is null;
}
```

#### 1.3 Batch Coordinator

```csharp
namespace NStore.Domain.Batch;

using System.Collections.Concurrent;
using System.Diagnostics;

/// <summary>
/// Coordinates parallel batch operations with retry and error handling
/// Uses .NET 10 features for optimal performance
/// </summary>
internal sealed class BatchOperationCoordinator
{
    private readonly SemaphoreSlim _semaphore;
    private readonly ILogger? _logger;

    public BatchOperationCoordinator(int maxParallelism, ILogger? logger = null)
    {
        _semaphore = new SemaphoreSlim(maxParallelism, maxParallelism);
        _logger = logger;
    }

    /// <summary>
    /// Execute operations in parallel with controlled concurrency
    /// </summary>
    public async Task<BatchResult<T>> ExecuteAsync<T>(
        IEnumerable<string> ids,
        Func<string, CancellationToken, Task<BatchItemResult<T>>> operation,
        BatchUpdateOptions options,
        CancellationToken cancellationToken) where T : IAggregate
    {
        var stopwatch = Stopwatch.StartNew();
        var results = new ConcurrentBag<BatchItemResult<T>>();
        var idList = ids.ToList();

        await Parallel.ForEachAsync(
            idList,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = options.MaxParallelism,
                CancellationToken = cancellationToken
            },
            async (id, ct) =>
            {
                await _semaphore.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    var result = await ExecuteWithRetryAsync(
                        id,
                        operation,
                        options,
                        ct
                    ).ConfigureAwait(false);

                    results.Add(result);

                    if (options.StopOnFirstError && result.Status == BatchItemStatus.Failed)
                    {
                        await cancellationToken.CancelAsync();
                    }
                }
                finally
                {
                    _semaphore.Release();
                }
            }
        );

        stopwatch.Stop();

        var resultList = results.ToList();
        return new BatchResult<T>
        {
            TotalCount = idList.Count,
            SuccessCount = resultList.Count(r => r.Status == BatchItemStatus.Success),
            FailureCount = resultList.Count(r => r.Status == BatchItemStatus.Failed),
            SkippedCount = resultList.Count(r => r.Status == BatchItemStatus.Skipped),
            Results = resultList,
            Duration = stopwatch.Elapsed
        };
    }

    private async Task<BatchItemResult<T>> ExecuteWithRetryAsync<T>(
        string id,
        Func<string, CancellationToken, Task<BatchItemResult<T>>> operation,
        BatchUpdateOptions options,
        CancellationToken cancellationToken) where T : IAggregate
    {
        int retryCount = 0;
        Exception? lastException = null;

        while (retryCount <= options.MaxRetries)
        {
            try
            {
                var result = await operation(id, cancellationToken).ConfigureAwait(false);

                if (result.Status == BatchItemStatus.Success ||
                    result.Status == BatchItemStatus.Skipped)
                {
                    return result with { RetryCount = retryCount };
                }

                // Failed, check if we should retry
                if (!IsRetryableError(result.Error) || retryCount >= options.MaxRetries)
                {
                    return result with { RetryCount = retryCount };
                }

                lastException = result.Error;
            }
            catch (Exception ex) when (IsRetryableError(ex) && retryCount < options.MaxRetries)
            {
                lastException = ex;
            }

            retryCount++;

            if (retryCount <= options.MaxRetries)
            {
                var delay = options.RetryDelay * Math.Pow(2, retryCount - 1);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        return new BatchItemResult<T>
        {
            AggregateId = id,
            Status = BatchItemStatus.Failed,
            Error = lastException,
            RetryCount = retryCount
        };
    }

    private static bool IsRetryableError(Exception? ex) => ex switch
    {
        ConcurrencyException => true,
        TimeoutException => true,
        _ when ex?.Message.Contains("deadlock", StringComparison.OrdinalIgnoreCase) == true => true,
        _ => false
    };
}
```

### Phase 2: Repository Implementation (Week 2)

#### 2.1 BatchRepository Implementation

```csharp
namespace NStore.Domain.Batch;

using System.Collections.Frozen;

/// <summary>
/// High-performance batch repository using .NET 10 features:
/// - Parallel.ForEachAsync for controlled parallelism
/// - IAsyncEnumerable for streaming results
/// - FrozenDictionary for aggregate lookup caching
/// </summary>
public sealed class BatchRepository : Repository, IBatchRepository
{
    private readonly BatchOperationCoordinator _coordinator;
    private readonly ILogger<BatchRepository>? _logger;

    public BatchRepository(
        IAggregateFactory factory,
        IStreamsFactory streams,
        ISnapshotStore? snapshots = null,
        int maxParallelism = 10,
        ILogger<BatchRepository>? logger = null)
        : base(factory, streams, snapshots)
    {
        _coordinator = new BatchOperationCoordinator(maxParallelism, logger);
        _logger = logger;
    }

    public async IAsyncEnumerable<BatchReadResult<T>> BatchReadAsync<T>(
        IEnumerable<string> aggregateIds,
        BatchReadOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : IAggregate
    {
        options ??= new BatchReadOptions();
        var idList = aggregateIds.ToList();

        _logger?.LogInformation(
            "Starting batch read of {Count} aggregates of type {Type}",
            idList.Count,
            typeof(T).Name
        );

        // Option 1: Stream results as they complete
        await foreach (var id in ProcessInParallel(idList, options.MaxParallelism))
        {
            BatchReadResult<T> result;
            try
            {
                var aggregate = await GetByIdAsync<T>(id, cancellationToken)
                    .ConfigureAwait(false);

                result = new BatchReadResult<T>
                {
                    AggregateId = id,
                    Aggregate = aggregate
                };
            }
            catch (Exception ex) when (options.IncludeNotFound || ex is not AggregateNotFoundException)
            {
                result = new BatchReadResult<T>
                {
                    AggregateId = id,
                    Error = ex
                };
            }

            yield return result;
        }
    }

    public async Task<BatchResult<T>> BatchUpdateAsync<T>(
        IEnumerable<string> aggregateIds,
        Action<T> updateAction,
        string operationId,
        BatchUpdateOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IAggregate
    {
        ArgumentNullException.ThrowIfNull(updateAction);
        ArgumentException.ThrowIfNullOrEmpty(operationId);

        options ??= new BatchUpdateOptions();
        var idList = aggregateIds.ToList();

        _logger?.LogInformation(
            "Starting batch update of {Count} aggregates of type {Type}",
            idList.Count,
            typeof(T).Name
        );

        return await _coordinator.ExecuteAsync<T>(
            idList,
            async (id, ct) => await UpdateSingleAsync(
                id,
                updateAction,
                operationId,
                options,
                ct
            ),
            options,
            cancellationToken
        ).ConfigureAwait(false);
    }

    private async Task<BatchItemResult<T>> UpdateSingleAsync<T>(
        string id,
        Action<T> updateAction,
        string operationId,
        BatchUpdateOptions options,
        CancellationToken cancellationToken) where T : IAggregate
    {
        try
        {
            var aggregate = await GetByIdAsync<T>(id, cancellationToken)
                .ConfigureAwait(false);

            // Check predicate if provided
            if (options.Predicate is not null && !options.Predicate(aggregate))
            {
                return new BatchItemResult<T>
                {
                    AggregateId = id,
                    Status = BatchItemStatus.Skipped,
                    Aggregate = aggregate
                };
            }

            // Apply update
            updateAction(aggregate);

            // Only save if dirty
            if (aggregate.IsDirty)
            {
                await SaveAsync(
                    aggregate,
                    $"{operationId}:{id}",
                    cancellationToken
                ).ConfigureAwait(false);
            }

            return new BatchItemResult<T>
            {
                AggregateId = id,
                Status = BatchItemStatus.Success,
                Aggregate = aggregate
            };
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Failed to update aggregate {Id} of type {Type}",
                id,
                typeof(T).Name
            );

            return new BatchItemResult<T>
            {
                AggregateId = id,
                Status = BatchItemStatus.Failed,
                Error = ex
            };
        }
    }

    public async Task<BatchResult<T>> BatchCreateAsync<T>(
        IEnumerable<(string Id, Action<T> InitAction)> items,
        string operationId,
        BatchCreateOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IAggregate
    {
        options ??= new BatchCreateOptions();
        var itemList = items.ToList();

        _logger?.LogInformation(
            "Starting batch create of {Count} aggregates of type {Type}",
            itemList.Count,
            typeof(T).Name
        );

        var stopwatch = Stopwatch.StartNew();
        var results = new ConcurrentBag<BatchItemResult<T>>();

        await Parallel.ForEachAsync(
            itemList,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = options.MaxParallelism,
                CancellationToken = cancellationToken
            },
            async (item, ct) =>
            {
                try
                {
                    var aggregate = _factory.Create<T>();
                    aggregate.Init(item.Id);
                    item.InitAction(aggregate);

                    await SaveAsync(
                        aggregate,
                        $"{operationId}:{item.Id}",
                        ct
                    ).ConfigureAwait(false);

                    results.Add(new BatchItemResult<T>
                    {
                        AggregateId = item.Id,
                        Status = BatchItemStatus.Success,
                        Aggregate = aggregate
                    });
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(
                        ex,
                        "Failed to create aggregate {Id} of type {Type}",
                        item.Id,
                        typeof(T).Name
                    );

                    results.Add(new BatchItemResult<T>
                    {
                        AggregateId = item.Id,
                        Status = BatchItemStatus.Failed,
                        Error = ex
                    });

                    if (!options.ContinueOnError)
                    {
                        await cancellationToken.CancelAsync();
                    }
                }
            }
        );

        stopwatch.Stop();

        var resultList = results.ToList();
        return new BatchResult<T>
        {
            TotalCount = itemList.Count,
            SuccessCount = resultList.Count(r => r.Status == BatchItemStatus.Success),
            FailureCount = resultList.Count(r => r.Status == BatchItemStatus.Failed),
            SkippedCount = 0,
            Results = resultList,
            Duration = stopwatch.Elapsed
        };
    }

    private static async IAsyncEnumerable<string> ProcessInParallel(
        List<string> ids,
        int maxParallelism)
    {
        var channel = Channel.CreateUnbounded<string>();

        _ = Task.Run(async () =>
        {
            await Parallel.ForEachAsync(
                ids,
                new ParallelOptions { MaxDegreeOfParallelism = maxParallelism },
                async (id, _) => await channel.Writer.WriteAsync(id)
            );
            channel.Writer.Complete();
        });

        await foreach (var id in channel.Reader.ReadAllAsync())
        {
            yield return id;
        }
    }
}
```

### Phase 3: Persistence Layer Enhancements (Week 3)

#### 3.1 Enhanced Persistence Interface

```csharp
namespace NStore.Core.Persistence;

/// <summary>
/// Extended persistence interface with batch operations
/// </summary>
public interface IBatchPersistence : IPersistence
{
    /// <summary>
    /// Read last chunk from multiple partitions efficiently (for batch updates)
    /// </summary>
    Task<IReadOnlyDictionary<string, IChunk?>> ReadMultipleLastAsync(
        IEnumerable<string> partitionIds,
        CancellationToken cancellationToken);

    /// <summary>
    /// Append to multiple partitions in parallel
    /// </summary>
    Task<IReadOnlyList<IChunk?>> AppendMultipleAsync(
        IEnumerable<(string PartitionId, long Index, object Payload, string OperationId)> operations,
        CancellationToken cancellationToken);
}
```

#### 3.2 MongoDB Batch Implementation

```csharp
namespace NStore.Persistence.Mongo;

public partial class MongoPersistence<TChunk> : IBatchPersistence
    where TChunk : IMongoChunk, new()
{
    public async Task<IReadOnlyDictionary<string, IChunk?>> ReadMultipleLastAsync(
        IEnumerable<string> partitionIds,
        CancellationToken cancellationToken)
    {
        var idList = partitionIds.ToList();
        var results = new Dictionary<string, IChunk?>();

        // Use aggregation pipeline for efficient batch query
        var pipeline = new[]
        {
            new BsonDocument("$match", new BsonDocument("PartitionId",
                new BsonDocument("$in", new BsonArray(idList)))),
            new BsonDocument("$sort", new BsonDocument
            {
                { "PartitionId", 1 },
                { "Index", -1 }
            }),
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$PartitionId" },
                { "lastChunk", new BsonDocument("$first", "$$ROOT") }
            })
        };

        var aggregateResult = await _chunks.Aggregate<BsonDocument>(pipeline, cancellationToken: cancellationToken)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        foreach (var doc in aggregateResult)
        {
            var partitionId = doc["_id"].AsString;
            var chunkDoc = doc["lastChunk"].AsBsonDocument;

            var chunk = BsonSerializer.Deserialize<TChunk>(chunkDoc);
            _mongoPayloadSerializer.ApplyDeserialization(chunk);

            results[partitionId] = chunk;
        }

        // Fill in nulls for partitions not found
        foreach (var id in idList.Where(id => !results.ContainsKey(id)))
        {
            results[id] = null;
        }

        return results.ToFrozenDictionary();
    }

    public async Task<IReadOnlyList<IChunk?>> AppendMultipleAsync(
        IEnumerable<(string PartitionId, long Index, object Payload, string OperationId)> operations,
        CancellationToken cancellationToken)
    {
        var opList = operations.ToList();
        var chunks = new List<TChunk>(opList.Count);

        // Get batch of IDs
        var firstId = await GetNextId(opList.Count, cancellationToken).ConfigureAwait(false);
        var lastId = firstId + opList.Count - 1;

        // Create chunks
        for (int i = 0; i < opList.Count; i++)
        {
            var op = opList[i];
            var chunk = new TChunk();
            chunk.Init(
                firstId + i,
                op.PartitionId,
                op.Index,
                _mongoPayloadSerializer.Serialize(op.Payload),
                op.OperationId ?? Guid.NewGuid().ToString()
            );
            chunks.Add(chunk);
        }

        var options = new InsertManyOptions { IsOrdered = false };

        try
        {
            await _chunks.InsertManyAsync(chunks, options, cancellationToken)
                .ConfigureAwait(false);

            return chunks.Cast<IChunk?>().ToList();
        }
        catch (MongoBulkWriteException<TChunk> ex)
        {
            // Handle partial failures
            var results = new IChunk?[opList.Count];
            var failedIndices = ex.WriteErrors.Select(e => e.Index).ToHashSet();

            for (int i = 0; i < opList.Count; i++)
            {
                results[i] = failedIndices.Contains(i) ? null : chunks[i];
            }

            return results;
        }
    }
}
```

#### 3.3 SQL Server Batch Implementation

```csharp
namespace NStore.Persistence.MsSql;

public partial class MsSqlPersistence : IBatchPersistence
{
    public async Task<IReadOnlyDictionary<string, IChunk?>> ReadMultipleLastAsync(
        IEnumerable<string> partitionIds,
        CancellationToken cancellationToken)
    {
        var idList = partitionIds.ToList();
        var results = new Dictionary<string, IChunk?>();

        var sql = $@"
            WITH LastChunks AS (
                SELECT
                    [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo],
                    ROW_NUMBER() OVER (PARTITION BY [PartitionId] ORDER BY [Index] DESC) as rn
                FROM {_options.StreamsTableName}
                WHERE [PartitionId] IN (SELECT value FROM STRING_SPLIT(@PartitionIds, ','))
            )
            SELECT [Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo]
            FROM LastChunks
            WHERE rn = 1";

        using var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false);
        using var command = context.CreateCommand(sql);

        context.AddParam(command, "@PartitionIds", string.Join(",", idList));

        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var chunk = ReadChunk(reader);
            results[chunk.PartitionId] = chunk;
        }

        // Fill in nulls
        foreach (var id in idList.Where(id => !results.ContainsKey(id)))
        {
            results[id] = null;
        }

        return results.ToFrozenDictionary();
    }

    public async Task<IReadOnlyList<IChunk?>> AppendMultipleAsync(
        IEnumerable<(string PartitionId, long Index, object Payload, string OperationId)> operations,
        CancellationToken cancellationToken)
    {
        var opList = operations.ToList();
        var results = new IChunk?[opList.Count];

        using var context = await _options.GetContextAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < opList.Count; i++)
        {
            var op = opList[i];
            var bytes = _options.Serializer.Serialize(op.Payload, out string serializerInfo);

            var sql = _options.GetInsertChunkSql();
            using var command = context.CreateCommand(sql);

            context.AddParam(command, "@PartitionId", op.PartitionId);
            context.AddParam(command, "@Index", op.Index);
            context.AddParam(command, "@OperationId", op.OperationId ?? Guid.NewGuid().ToString());
            context.AddParam(command, "@Payload", bytes);
            context.AddParam(command, "@SerializerInfo", serializerInfo);

            var position = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            results[i] = new SqlChunk
            {
                Position = position,
                PartitionId = op.PartitionId,
                Index = op.Index,
                Payload = op.Payload,
                OperationId = op.OperationId,
                SerializerInfo = serializerInfo
            };
        }

        return results;
    }
}
```

---

## Performance Optimizations

### 1. Connection Pooling

```csharp
// Reuse connections across batch operations
public sealed class BatchConnectionManager : IAsyncDisposable
{
    private readonly ConcurrentBag<IDbConnection> _pool = new();
    private readonly SemaphoreSlim _semaphore;

    public async Task<IDbConnection> AcquireAsync(CancellationToken ct)
    {
        await _semaphore.WaitAsync(ct);
        return _pool.TryTake(out var conn) ? conn : await CreateNewAsync();
    }

    public void Release(IDbConnection connection)
    {
        _pool.Add(connection);
        _semaphore.Release();
    }
}
```

### 2. Result Streaming

Use `IAsyncEnumerable<T>` to stream results without buffering all in memory:

```csharp
await foreach (var result in repository.BatchReadAsync<Order>(orderIds))
{
    ProcessResult(result); // Process as results arrive
}
```

### 3. Aggregation Pushdown

For MongoDB, use aggregation pipeline to compute results server-side:

```csharp
// Calculate total directly in MongoDB
var pipeline = new[]
{
    new BsonDocument("$match", new BsonDocument("status", "completed")),
    new BsonDocument("$group", new BsonDocument
    {
        { "_id", BsonNull.Value },
        { "total", new BsonDocument("$sum", "$amount") }
    })
};
```

### 4. Parallel Query Execution

Execute independent queries in parallel:

```csharp
var tasks = partitionGroups.Select(group =>
    persistence.ReadForwardAsync(group, subscription, cancellationToken)
);

await Task.WhenAll(tasks);
```

---

## Error Handling

### Retry Strategy

```csharp
public sealed record RetryPolicy
{
    public int MaxRetries { get; init; } = 3;
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public double BackoffMultiplier { get; init; } = 2.0;
    public Func<Exception, bool> ShouldRetry { get; init; } = DefaultShouldRetry;

    private static bool DefaultShouldRetry(Exception ex) => ex is
        ConcurrencyException or
        TimeoutException or
        DbException { IsTransient: true };
}
```

---

## Testing Strategy

### Unit Tests

```csharp
[Fact]
public async Task BatchUpdate_WithAllSuccessful_ReturnsSuccessResult()
{
    // Arrange
    var repository = CreateBatchRepository();
    var ids = Enumerable.Range(1, 100).Select(i => $"order-{i}").ToList();

    // Act
    var result = await repository.BatchUpdateAsync<Order>(
        ids,
        order => order.Complete(),
        "test-op"
    );

    // Assert
    result.IsSuccess.Should().BeTrue();
    result.SuccessCount.Should().Be(100);
    result.FailureCount.Should().Be(0);
}

[Fact]
public async Task BatchUpdate_WithPartialFailure_ReturnsPartialSuccess()
{
    // Arrange
    var repository = CreateBatchRepository();
    var ids = new[] { "valid-1", "invalid", "valid-2" };

    // Act
    var result = await repository.BatchUpdateAsync<Order>(
        ids,
        order => order.Complete(),
        "test-op",
        new BatchUpdateOptions { StopOnFirstError = false }
    );

    // Assert
    result.IsPartialSuccess.Should().BeTrue();
    result.SuccessCount.Should().Be(2);
    result.FailureCount.Should().Be(1);
}
```

### Performance Tests

```csharp
[Fact]
public async Task BatchUpdate_100Items_CompletesUnder1Second()
{
    var stopwatch = Stopwatch.StartNew();

    await repository.BatchUpdateAsync<Order>(
        Enumerable.Range(1, 100).Select(i => $"order-{i}"),
        order => order.Complete(),
        "perf-test"
    );

    stopwatch.Stop();
    stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1));
}
```

### Integration Tests

```csharp
[Fact]
public async Task BatchUpdate_MongoDB_ReducesRoundtrips()
{
    // Measure roundtrips using profiler
    using var profiler = new MongoDbProfiler();

    await repository.BatchUpdateAsync<Order>(orderIds,
        order => order.Complete(),
        "integration-test"
    );

    profiler.CommandCount.Should().BeLessThan(10); // vs 100 without batching
}
```

---

## Migration Guide

### Step 1: Update Dependencies

```xml
<PackageReference Include="NStore.Domain" Version="2.0.0" />
<PackageReference Include="NStore.Domain.Batch" Version="2.0.0" />
```

### Step 2: Replace Sequential Loops

**Before**:
```csharp
foreach (var orderId in orderIds)
{
    var order = await repository.GetByIdAsync<Order>(orderId);
    order.Complete();
    await repository.SaveAsync(order, operationId);
}
```

**After**:
```csharp
var result = await batchRepository.BatchUpdateAsync<Order>(
    orderIds,
    order => order.Complete(),
    operationId
);
```

### Step 3: Handle Results

```csharp
if (result.IsSuccess)
{
    _logger.LogInformation("All {Count} orders completed", result.SuccessCount);
}
else if (result.IsPartialSuccess)
{
    _logger.LogWarning(
        "Partial success: {Success}/{Total}. Failures: {Failures}",
        result.SuccessCount,
        result.TotalCount,
        string.Join(", ", result.Failures.Select(f => f.AggregateId))
    );
}
```

---

## Benchmarks

Expected performance improvements:

| Operation | Items | Sequential | Batch | Improvement |
|-----------|-------|------------|-------|-------------|
| Update (MongoDB) | 100 | 2.5s | 250ms | 10x faster |
| Update (SQL) | 100 | 3.0s | 400ms | 7.5x faster |
| Read (MongoDB) | 100 | 1.8s | 180ms | 10x faster |
| Create (MongoDB) | 1000 | 15s | 2s | 7.5x faster |

---

## Future Enhancements

1. **Event Streaming**: Real-time event streaming from batch operations
2. **Metrics & Telemetry**: OpenTelemetry integration for observability
3. **GraphQL Support**: Batch operations via GraphQL DataLoader pattern

---

*Document maintained by: NStore Development Team*
*Created: 2024-12-22*
*Target Runtime: .NET 10 / C# 13*
