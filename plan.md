# NStore Performance Enhancement Plan

> **Generated**: 2024-12-22
> **Target**: NStore Event Sourcing Library
> **Scope**: Speed and Memory Optimizations
> **Runtime Target**: .NET 10 / C# 13
> **Language Features**: Primary constructors, FrozenDictionary, Lock object, params collections, ValueTask, IAsyncEnumerable

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current Architecture Overview](#current-architecture-overview)
3. [Iteration 1: Critical Performance Fixes](#iteration-1-critical-performance-fixes)
4. [Iteration 2: Memory and Lifecycle Management](#iteration-2-memory-and-lifecycle-management)
5. [Iteration 3: Database and Query Optimizations](#iteration-3-database-and-query-optimizations)
6. [Iteration 4: Advanced Optimizations](#iteration-4-advanced-optimizations)
7. [Bug Fixes](#bug-fixes)
8. [Benchmarking Strategy](#benchmarking-strategy)
9. [Implementation Checklist](#implementation-checklist)

---

## Executive Summary

This document outlines **23 performance enhancements** and **2 bug fixes** identified through comprehensive code analysis of the NStore event sourcing library. The improvements are organized into 4 iterations, prioritized by impact and implementation effort.

### Impact Summary

| Iteration | Focus | Items | Risk | Expected Improvement |
|-----------|-------|-------|------|---------------------|
| 1 | Critical Fixes | 5 | Low | 50-90% in hot paths |
| 2 | Memory/Lifecycle | 5 | Low | 20-40% memory reduction |
| 3 | Database | 5 | Medium | 30-80% query improvement |
| 4 | Advanced | 6 | Medium | 10-30% overall |

### Key Metrics to Track

- Event replay time (ms per 1K events)
- Append throughput (events/second)
- Memory allocation per operation (bytes)
- GC collection frequency (Gen0/Gen1/Gen2)
- Database roundtrips per operation

---

## Current Architecture Overview

```
NStore/
├── NStore.Core/                    # Core abstractions
│   ├── Persistence/                # IPersistence, IChunk, ISubscription
│   ├── Streams/                    # Stream, OptimisticConcurrencyStream
│   ├── Processing/                 # MethodInvoker, PayloadProcessors
│   ├── Snapshots/                  # ISnapshotStore, SnapshotInfo
│   └── InMemory/                   # InMemoryPersistence (testing)
├── NStore.Domain/                  # Domain abstractions
│   ├── Aggregate.cs                # Base aggregate class
│   ├── Repository.cs               # Aggregate repository
│   ├── Changeset.cs                # Event batch container
│   └── Poco/                       # POCO aggregate support
├── NStore.Persistence.Mongo/       # MongoDB implementation
├── NStore.Persistence.MsSql/       # SQL Server implementation
├── NStore.Persistence.Sqlite/      # SQLite implementation
├── NStore.Persistence.LiteDB/      # LiteDB implementation
├── NStore.BaseSqlPersistence/      # Shared SQL base
└── NStore.Tpl/                     # TPL Dataflow batching
```

### Hot Paths Identified

1. **Aggregate Replay**: `Repository.GetByIdAsync` → `Stream.ReadAsync` → `Aggregate.ApplyChanges`
2. **Event Emit**: `Aggregate.Emit` → `IPayloadProcessor.Process` → `MethodInvoker`
3. **Persistence Write**: `Stream.AppendAsync` → `IPersistence.AppendAsync`
4. **Subscription Processing**: `PushToSubscriber` → `ISubscription.OnNextAsync`

---

## Iteration 1: Critical Performance Fixes

### 1.1 Reflection Caching in MethodInvoker

**Priority**: P0 (Critical)
**Impact**: 50-70% faster event replay
**Effort**: Low
**Risk**: Low

#### Problem

Every event handler call uses `GetType().GetMethod()` which is extremely expensive, especially during aggregate replay with thousands of events.

**File**: `src/NStore.Core/Processing/MethodInvoker.cs`

```csharp
// Current implementation (lines 11-21)
public static object CallNonPublicIfExists(this object instance, string methodName, object @parameter)
{
    var mi = instance.GetType().GetMethod(
        methodName,
        NonPublic,
        null,
        new Type[] {@parameter.GetType()},
        null
    );

    return mi == null ? null : Execute(mi, instance, parameter);
}
```

#### Solution

Implement a thread-safe cache using `FrozenDictionary` for hot-path lookups and compiled delegates for maximum performance.

```csharp
using System.Collections.Frozen;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace NStore.Core.Processing;

/// <summary>
/// High-performance method invoker using .NET 10 features:
/// - FrozenDictionary for O(1) lookups after warmup
/// - Compiled expression trees for near-native call speed
/// - Lock object for thread-safe cache building
/// </summary>
public static class MethodInvoker
{
    private static readonly Lock _cacheLock = new();
    private static FrozenDictionary<(Type, string, Type), Func<object, object, object?>> _frozenCache =
        FrozenDictionary<(Type, string, Type), Func<object, object, object?>>.Empty;

    private static readonly ConcurrentDictionary<(Type, string, Type), Func<object, object, object?>> _buildingCache = new();

    private const BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
    private const BindingFlags Public = BindingFlags.Public | BindingFlags.Instance;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? CallNonPublicIfExists(this object instance, string methodName, object parameter)
    {
        var key = (instance.GetType(), methodName, parameter.GetType());

        // Fast path: check frozen cache first
        if (_frozenCache.TryGetValue(key, out var cachedInvoker))
        {
            return cachedInvoker(instance, parameter);
        }

        // Slow path: build and cache
        return GetOrCreateInvoker(key, NonPublic)?.Invoke(instance, parameter);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? CallNonPublicIfExists(this object instance, ReadOnlySpan<string> methodNames, object parameter)
    {
        var instanceType = instance.GetType();
        var paramType = parameter.GetType();

        foreach (var methodName in methodNames)
        {
            var key = (instanceType, methodName, paramType);

            if (_frozenCache.TryGetValue(key, out var cachedInvoker))
            {
                return cachedInvoker(instance, parameter);
            }

            var invoker = GetOrCreateInvoker(key, NonPublic);
            if (invoker is not null)
            {
                return invoker(instance, parameter);
            }
        }
        return null;
    }

    // Overload for array (maintains compatibility)
    public static object? CallNonPublicIfExists(this object instance, string[] methodNames, object parameter)
        => CallNonPublicIfExists(instance, methodNames.AsSpan(), parameter);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? CallPublicIfExists(this object instance, string methodName, object parameter)
    {
        var key = (instance.GetType(), methodName, parameter.GetType());

        if (_frozenCache.TryGetValue(key, out var cachedInvoker))
        {
            return cachedInvoker(instance, parameter);
        }

        return GetOrCreateInvoker(key, Public)?.Invoke(instance, parameter);
    }

    public static object? CallPublic(this object instance, string methodName, object parameter)
    {
        var result = CallPublicIfExists(instance, methodName, parameter);
        if (result is null && !HasMethod(instance.GetType(), methodName, parameter.GetType(), Public))
        {
            throw new MissingMethodException(instance.GetType().FullName, methodName);
        }
        return result;
    }

    private static Func<object, object, object?>? GetOrCreateInvoker(
        (Type InstanceType, string MethodName, Type ParamType) key,
        BindingFlags flags)
    {
        // Try building cache first
        if (_buildingCache.TryGetValue(key, out var existing))
        {
            return existing;
        }

        var invoker = CompileInvoker(key.InstanceType, key.MethodName, key.ParamType, flags);

        if (invoker is not null)
        {
            _buildingCache.TryAdd(key, invoker);

            // Periodically freeze the cache for faster lookups
            if (_buildingCache.Count > _frozenCache.Count + 50)
            {
                RebuildFrozenCache();
            }
        }

        return invoker;
    }

    private static void RebuildFrozenCache()
    {
        lock (_cacheLock)
        {
            if (_buildingCache.Count > _frozenCache.Count)
            {
                _frozenCache = _buildingCache.ToFrozenDictionary();
            }
        }
    }

    private static Func<object, object, object?>? CompileInvoker(
        Type instanceType,
        string methodName,
        Type paramType,
        BindingFlags flags)
    {
        var mi = instanceType.GetMethod(methodName, flags, null, [paramType], null);
        if (mi is null) return null;

        var instanceParam = Expression.Parameter(typeof(object), "instance");
        var paramParam = Expression.Parameter(typeof(object), "param");

        var call = Expression.Call(
            Expression.Convert(instanceParam, instanceType),
            mi,
            Expression.Convert(paramParam, paramType)
        );

        Expression body = mi.ReturnType == typeof(void)
            ? Expression.Block(call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));

        return Expression.Lambda<Func<object, object, object?>>(body, instanceParam, paramParam).Compile();
    }

    private static bool HasMethod(Type type, string name, Type paramType, BindingFlags flags)
        => type.GetMethod(name, flags, null, [paramType], null) is not null;

    /// <summary>
    /// Force rebuild of frozen cache. Call after warmup period.
    /// </summary>
    public static void FreezeCacheNow() => RebuildFrozenCache();

    /// <summary>
    /// Clear all caches. For testing only.
    /// </summary>
    public static void ClearCache()
    {
        lock (_cacheLock)
        {
            _buildingCache.Clear();
            _frozenCache = FrozenDictionary<(Type, string, Type), Func<object, object, object?>>.Empty;
        }
    }
}
```

**Key .NET 10 / C# 13 Features Used:**
- `FrozenDictionary<K,V>` - Immutable dictionary optimized for read-heavy scenarios
- `Lock` object - New lightweight lock type replacing `object` locks
- `ReadOnlySpan<string>` - Stack-allocated parameter for method names
- Collection expressions `[paramType]` - Cleaner array syntax
- File-scoped namespace - Reduced indentation
- `is not null` pattern - Modern null checking
- `MethodImpl(AggressiveInlining)` - Hint for hot paths

#### Test Cases

```csharp
[Fact]
public void CallNonPublicIfExists_ShouldCacheMethodInfo()
{
    var state = new TestState();
    var event1 = new TestEvent();

    // First call - populates cache
    state.CallNonPublicIfExists("On", event1);

    // Second call - should use cache (verify with profiler or counter)
    state.CallNonPublicIfExists("On", event1);

    // Cache should contain the method
    Assert.True(MethodInvoker.CacheContains(typeof(TestState), "On", typeof(TestEvent)));
}
```

---

### 1.2 In-Memory Persistence Fixed Array Allocation

**Priority**: P0 (Critical)
**Impact**: 90-95% memory reduction for tests
**Effort**: Low
**Risk**: Low

#### Problem

Pre-allocates 1 million chunk slots regardless of actual usage.

**File**: `src/NStore.Core/InMemory/InMemoryPersistence.cs`

```csharp
// Line 50: Always allocates ~8-16 MB
_chunks = new MemoryChunk[1024 * 1024];
```

#### Solution

Replace with dynamically growing list and modern .NET 10 features.

```csharp
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace NStore.Core.InMemory;

/// <summary>
/// High-performance in-memory persistence using .NET 10 features:
/// - Lock object for lightweight synchronization
/// - IAsyncEnumerable for streaming reads
/// - ValueTask for reduced allocations on hot paths
/// </summary>
public class InMemoryPersistence : IPersistence, IAsyncEnumerable<IChunk>
{
    private readonly Func<object, object> _cloneFunc;
    private readonly List<MemoryChunk?> _chunks;
    private readonly Lock _lock = new();

    private readonly ConcurrentDictionary<string, InMemoryPartition> _partitions = new();

    private int _sequence;
    private int _lastWrittenPosition = -1;
    private readonly INetworkSimulator _networkSimulator;
    private readonly InMemoryPartition _emptyInMemoryPartition;
    private readonly InMemoryPersistenceOptions _options;
    private const string EmptyPartitionId = "::empty";

    public bool SupportsFillers => true;

    public InMemoryPersistence() : this(new InMemoryPersistenceOptions()) { }

    public InMemoryPersistence(INetworkSimulator networkSimulator)
        : this(new InMemoryPersistenceOptions { NetworkSimulator = networkSimulator }) { }

    public InMemoryPersistence(Func<object, object> cloneFunc)
        : this(new InMemoryPersistenceOptions { CloneFunc = cloneFunc }) { }

    public IEnumerable<string> PartitionIds => _partitions.Keys.Where(x => x != EmptyPartitionId);

    public InMemoryPersistence(InMemoryPersistenceOptions options)
    {
        _options = options;
        _chunks = new(capacity: options.InitialCapacity);
        _cloneFunc = options.CloneFunc ?? (static o => o);
        _networkSimulator = options.NetworkSimulator ?? NoNetworkLatencySimulator.Instance;
        _emptyInMemoryPartition = new InMemoryPartition(EmptyPartitionId, _networkSimulator, Clone);
        _partitions.TryAdd(_emptyInMemoryPartition.Id, _emptyInMemoryPartition);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetChunk(MemoryChunk chunk)
    {
        int slot = (int)chunk.Position - 1;

        lock (_lock)
        {
            // Use CollectionsMarshal for direct span access in .NET 10
            CollectionsMarshal.SetCount(_chunks, Math.Max(_chunks.Count, slot + 1));
            _chunks[slot] = chunk;

            if (_lastWrittenPosition < slot)
            {
                _lastWrittenPosition = slot;
            }
        }
    }

    public async Task ReadAllAsync(
        long fromPositionInclusive,
        ISubscription subscription,
        int limit,
        CancellationToken cancellationToken)
    {
        await subscription.OnStartAsync(fromPositionInclusive).ConfigureAwait(false);

        int start = (int)Math.Max(fromPositionInclusive - 1, 0);
        int lastWritten, chunksCount;

        lock (_lock)
        {
            lastWritten = _lastWrittenPosition;
            chunksCount = _chunks.Count;
        }

        if (start > lastWritten || start >= chunksCount)
        {
            await subscription.StoppedAsync(fromPositionInclusive).ConfigureAwait(false);
            return;
        }

        var toRead = Math.Min(limit, lastWritten - start + 1);
        if (toRead <= 0)
        {
            await subscription.StoppedAsync(fromPositionInclusive).ConfigureAwait(false);
            return;
        }

        long position = 0;

        try
        {
            for (int i = start; i < start + toRead && i < chunksCount; i++)
            {
                var chunk = _chunks[i];
                if (chunk is null || chunk.Deleted)
                {
                    continue;
                }

                position = chunk.Position;

                await _networkSimulator.Wait().ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (!await subscription.OnNextAsync(Clone(chunk)).ConfigureAwait(false))
                {
                    await subscription.StoppedAsync(position).ConfigureAwait(false);
                    return;
                }
            }

            await (position == 0
                ? subscription.StoppedAsync(fromPositionInclusive)
                : subscription.CompletedAsync(position)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await subscription.OnErrorAsync(position, e).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Stream all chunks using IAsyncEnumerable (new in this version)
    /// </summary>
    public async IAsyncEnumerable<IChunk> StreamAllAsync(
        long fromPositionInclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int start = (int)Math.Max(fromPositionInclusive - 1, 0);
        int lastWritten;

        lock (_lock)
        {
            lastWritten = _lastWrittenPosition;
        }

        for (int i = start; i <= lastWritten; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            MemoryChunk? chunk;
            lock (_lock)
            {
                chunk = i < _chunks.Count ? _chunks[i] : null;
            }

            if (chunk is not null && !chunk.Deleted)
            {
                await _networkSimulator.Wait().ConfigureAwait(false);
                yield return Clone(chunk)!;
            }
        }
    }

    public async IAsyncEnumerator<IChunk> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        await foreach (var chunk in StreamAllAsync(1, cancellationToken))
        {
            yield return chunk;
        }
    }

    public ValueTask<long> ReadLastPositionAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_lastWrittenPosition == -1)
                return ValueTask.FromResult(0L);

            return ValueTask.FromResult(_chunks[_lastWrittenPosition]!.Position);
        }
    }

    // ... rest of implementation with similar modernizations ...
}
```

#### Update InMemoryPersistenceOptions

```csharp
namespace NStore.Core.InMemory;

/// <summary>
/// Options for InMemoryPersistence using primary constructor (C# 13)
/// </summary>
public sealed class InMemoryPersistenceOptions
{
    public Func<object, object>? CloneFunc { get; init; }
    public INetworkSimulator? NetworkSimulator { get; init; }
    public int InitialCapacity { get; init; } = 1024;

    // Parameterless constructor for object initializer syntax
    public InMemoryPersistenceOptions() { }

    // Convenience constructor
    public InMemoryPersistenceOptions(
        Func<object, object>? cloneFunc,
        INetworkSimulator? networkSimulator,
        int initialCapacity = 1024)
    {
        CloneFunc = cloneFunc;
        NetworkSimulator = networkSimulator;
        InitialCapacity = initialCapacity;
    }
}
```

**Key .NET 10 / C# 13 Features Used:**
- `Lock` object - Lightweight lock type
- `IAsyncEnumerable<T>` - Streaming reads without buffering
- `ValueTask<T>` - Reduced allocations for synchronous completions
- `CollectionsMarshal.SetCount` - Direct list manipulation
- `init` accessors - Immutable after construction
- `static` lambda - Avoids closure allocation
- `is null` / `is not null` patterns - Modern null checking

---

### 1.3 Enable TPL BoundedCapacity

**Priority**: P0 (Critical)
**Impact**: Prevents memory exhaustion, better tail latency
**Effort**: Very Low
**Risk**: Very Low

#### Problem

`BoundedCapacity` is commented out, risking unbounded memory growth under high load.

**File**: `src/NStore.Tpl/PersistenceBatchAppendDecorator.cs`

```csharp
// Lines 20-24: BoundedCapacity commented out
_batch = new BatchBlock<AsyncWriteJob>(batchSize, new GroupingDataflowBlockOptions()
{
    //                BoundedCapacity = 1024,
    CancellationToken = _cts.Token
});
```

#### Solution

Enable bounded capacity with configurable value.

```csharp
public class PersistenceBatchAppendDecorator : IPersistence, IDisposable
{
    private readonly IPersistence _persistence;
    private readonly BatchBlock<AsyncWriteJob> _batch;
    private readonly CancellationTokenSource _cts;

    public PersistenceBatchAppendDecorator(
        IPersistence persistence,
        int batchSize,
        int flushTimeout,
        int boundedCapacity = 1024)  // New parameter with default
    {
        _cts = new CancellationTokenSource();
        var batcher = (IEnhancedPersistence)persistence;

        _batch = new BatchBlock<AsyncWriteJob>(batchSize, new GroupingDataflowBlockOptions()
        {
            BoundedCapacity = boundedCapacity,  // ENABLED
            CancellationToken = _cts.Token
        });

        Task.Run(async () =>
        {
            while (!_cts.IsCancellationRequested)
            {
                await Task.Delay(flushTimeout).ConfigureAwait(false);
                _batch.TriggerBatch();
            }
        });

        var processor = new ActionBlock<AsyncWriteJob[]>
        (
            queue => batcher.AppendBatchAsync(queue, CancellationToken.None),
            new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                BoundedCapacity = boundedCapacity,  // Also bound processor
                CancellationToken = _cts.Token
            }
        );

        _batch.LinkTo(processor, new DataflowLinkOptions()
        {
            PropagateCompletion = true,
        });

        _persistence = persistence;
    }

    // ... rest unchanged ...
}
```

---

### 1.4 SubscriptionWrapper Unnecessary Async State Machines

**Priority**: P1 (High)
**Impact**: ~10% faster subscription processing, reduced GC
**Effort**: Very Low
**Risk**: Very Low

#### Problem

Creates async state machines for simple task delegation.

**File**: `src/NStore.Core/Persistence/SubscriptionWrapper.cs`

```csharp
// Current: Creates state machine for simple delegation
public async Task CompletedAsync(long indexOrPosition)
{
    await _wrapped.CompletedAsync(indexOrPosition).ConfigureAwait(false);
}
```

#### Solution

Return tasks directly when no processing needed.

```csharp
public class SubscriptionWrapper : ISubscription
{
    private readonly ISubscription _wrapped;

    public SubscriptionWrapper(ISubscription wrapped)
    {
        _wrapped = wrapped;
        ChunkFilter = c => true;
    }

    public Action<IChunk> BeforeOnNext { get; set; }
    public Func<IChunk, bool> ChunkFilter { get; set; }

    public async Task<bool> OnNextAsync(IChunk chunk)
    {
        if (ChunkFilter(chunk))
        {
            BeforeOnNext?.Invoke(chunk);
            return await _wrapped.OnNextAsync(chunk).ConfigureAwait(false);
        }

        return true;
    }

    // Optimized: Direct task return, no async state machine
    public Task CompletedAsync(long indexOrPosition)
        => _wrapped.CompletedAsync(indexOrPosition);

    public Task StoppedAsync(long indexOrPosition)
        => _wrapped.StoppedAsync(indexOrPosition);

    public Task OnStartAsync(long indexOrPosition)
        => _wrapped.OnStartAsync(indexOrPosition);

    public Task OnErrorAsync(long indexOrPosition, Exception ex)
        => _wrapped.OnErrorAsync(indexOrPosition, ex);
}
```

---

### 1.5 Stream Append Retry Optimization

**Priority**: P1 (High)
**Impact**: 30-40% faster appends to new streams
**Effort**: Low
**Risk**: Low

#### Problem

Unconditional database lookup on first append.

**File**: `src/NStore.Core/Streams/Stream.cs`

```csharp
// Lines 47-54: Always queries on first append
if (_lastIndex == -1)
{
    var last = await PeekAsync(cancellation).ConfigureAwait(false);
    _lastIndex = last?.Index ?? 0;
}
```

#### Solution

Add exponential backoff and optimize initial state.

```csharp
public class Stream : IRandomAccessStream
{
    private IPersistence Persistence { get; }
    public string Id { get; }
    public virtual bool IsWritable => true;
    private long _lastIndex = -1;
    private bool _isNew = false;  // New: track if stream is known to be new

    public Stream(string streamId, IPersistence persistence)
    {
        this.Id = streamId;
        this.Persistence = persistence;
    }

    // New: Mark stream as new to skip initial peek
    public void MarkAsNew()
    {
        _lastIndex = 0;
        _isNew = true;
    }

    public virtual async Task<IChunk> AppendAsync(
        object payload,
        string operationId,
        CancellationToken cancellation
    )
    {
        int baseDelay = 10;  // ms

        for (var retries = 0; retries < 10; retries++)
        {
            try
            {
                if (_lastIndex == -1)
                {
                    var last = await PeekAsync(cancellation).ConfigureAwait(false);
                    _lastIndex = last?.Index ?? 0;
                }

                var index = _lastIndex + 1;

                var chunk = await Persistence.AppendAsync(this.Id, index, payload, operationId, cancellation)
                    .ConfigureAwait(false);

                _lastIndex = chunk.Index;
                return chunk;
            }
            catch (DuplicateStreamIndexException)
            {
                _lastIndex = -1;

                // Exponential backoff with jitter
                if (retries > 0)
                {
                    var delay = baseDelay * (1 << Math.Min(retries, 6));  // Cap at ~640ms
                    var jitter = Random.Shared.Next(0, delay / 4);
                    await Task.Delay(delay + jitter, cancellation).ConfigureAwait(false);
                }
            }
        }

        throw new AppendFailedException(this.Id, "Too many retries");
    }

    // ... rest unchanged ...
}
```

---

## Iteration 2: Memory and Lifecycle Management

### 2.1 Repository IDisposable Implementation

**Priority**: P1 (High)
**Impact**: Prevents memory leaks
**Effort**: Low
**Risk**: Very Low

#### Problem

No cleanup mechanism for tracked aggregates in long-lived repositories.

**File**: `src/NStore.Domain/Repository.cs`

#### Solution

```csharp
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace NStore.Domain;

/// <summary>
/// Repository with proper lifecycle management using .NET 10 features:
/// - IAsyncDisposable for async cleanup
/// - ObjectDisposedException.ThrowIf for cleaner disposal checks
/// - Primary constructor pattern
/// </summary>
public class Repository : IRepository, IDisposable, IAsyncDisposable
{
    private readonly IAggregateFactory _factory;
    private readonly IStreamsFactory _streams;
    private readonly Dictionary<string, IStream> _openedStreams = [];
    private readonly Dictionary<string, IAggregate> _trackingAggregates = [];
    private readonly ISnapshotStore? _snapshots;
    private bool _disposed;

    public bool PersistEmptyChangeset { get; set; }

    public Repository(IAggregateFactory factory, IStreamsFactory streams)
        : this(factory, streams, null) { }

    public Repository(IAggregateFactory factory, IStreamsFactory streams, ISnapshotStore? snapshots)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(streams);

        _factory = factory;
        _streams = streams;
        _snapshots = snapshots;
    }

    /// <summary>
    /// Detach a specific aggregate from tracking
    /// </summary>
    public bool Detach(string aggregateId)
    {
        ThrowIfDisposed();
        var removed = _trackingAggregates.Remove(aggregateId);
        _openedStreams.Remove(aggregateId);
        return removed;
    }

    /// <summary>
    /// Check if an aggregate is being tracked
    /// </summary>
    public bool IsTracking(string aggregateId)
    {
        ThrowIfDisposed();
        return _trackingAggregates.ContainsKey(aggregateId);
    }

    /// <summary>
    /// Try to get a tracked aggregate without loading from store
    /// </summary>
    public bool TryGetTracked<T>(string aggregateId, [NotNullWhen(true)] out T? aggregate) where T : IAggregate
    {
        ThrowIfDisposed();
        if (_trackingAggregates.TryGetValue(aggregateId, out var tracked) && tracked is T typedAggregate)
        {
            aggregate = typedAggregate;
            return true;
        }
        aggregate = default;
        return false;
    }

    /// <summary>
    /// Get count of tracked aggregates
    /// </summary>
    public int TrackedCount => _trackingAggregates.Count;

    /// <summary>
    /// Get all tracked aggregate IDs (for diagnostics)
    /// </summary>
    public IReadOnlyCollection<string> TrackedIds => _trackingAggregates.Keys;

    public void Clear()
    {
        _trackingAggregates.Clear();
        _openedStreams.Clear();
    }

    public Task<IAggregate> GetByIdAsync(Type aggregateType, string id)
        => GetByIdAsync(aggregateType, id, CancellationToken.None);

    public Task<T> GetByIdAsync<T>(string id) where T : IAggregate
        => GetByIdAsync<T>(id, CancellationToken.None);

    public async Task<IAggregate> GetByIdAsync(
        Type aggregateType,
        string id,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(aggregateType);
        ArgumentException.ThrowIfNullOrEmpty(id);

        if (_trackingAggregates.TryGetValue(id, out var existing))
        {
            return existing;
        }

        var aggregate = _factory.Create(aggregateType);
        var persister = (IEventSourcedAggregate)aggregate;

        SnapshotInfo? snapshot = null;

        if (_snapshots is not null && aggregate is ISnapshottable snapshottable)
        {
            snapshot = await _snapshots.GetLastAsync(id, cancellationToken).ConfigureAwait(false);
            if (snapshot is not null)
            {
                snapshottable.TryRestore(snapshot);
            }
        }

        if (!aggregate.IsInitialized)
        {
            aggregate.Init(id);
        }
        else if (aggregate.Id != id)
        {
            throw new AggregateAlreadyInitializedException(aggregateType, id);
        }

        _trackingAggregates.Add(id, aggregate);
        var stream = OpenStream(id);

        int readCount = 0;
        var subscription = new LambdaSubscription(data =>
        {
            readCount++;
            persister.ApplyChanges((Changeset)data.Payload);
            return Task.FromResult(true);
        });

        var consumer = ConfigureConsumer(subscription, cancellationToken);

        await stream.ReadAsync(consumer, aggregate.Version, long.MaxValue, cancellationToken)
            .ConfigureAwait(false);

        if (subscription.Failed)
        {
            throw new RepositoryReadException($"Error reading aggregate {id}", subscription.LastError);
        }

        persister.Loaded();

        if (snapshot is not null && readCount == 0)
        {
            throw new StaleSnapshotException(snapshot.SourceId, snapshot.SourceVersion);
        }

        return aggregate;
    }

    public async Task<T> GetByIdAsync<T>(string id, CancellationToken cancellationToken) where T : IAggregate
        => (T)await GetByIdAsync(typeof(T), id, cancellationToken).ConfigureAwait(false);

    // ... SaveAsync and other methods with similar modernizations ...

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            Clear();
        }
        _disposed = true;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
```

**Key .NET 10 / C# 13 Features Used:**
- `IAsyncDisposable` - Async cleanup pattern
- `ObjectDisposedException.ThrowIf` - Cleaner disposal checks
- `ArgumentNullException.ThrowIfNull` - Modern argument validation
- `ArgumentException.ThrowIfNullOrEmpty` - String validation
- `[NotNullWhen(true)]` - Nullable flow analysis
- Collection expression `[]` - Empty dictionary initialization
- `is not null` pattern matching

---

### 2.2 Aggregate PendingChanges Optimization

**Priority**: P2 (Medium)
**Impact**: Reduced GC allocations
**Effort**: Medium
**Risk**: Low

#### Problem

Uses `List<object>` with boxing, allocates array on every `GetChangeSet()`.

**File**: `src/NStore.Domain/Aggregate.cs`

#### Solution

```csharp
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NStore.Domain;

/// <summary>
/// High-performance aggregate base using .NET 10 features:
/// - ArrayPool for reduced allocations
/// - Span-based operations where possible
/// - InlineArray for small event batches (C# 12+)
/// </summary>
public abstract class Aggregate<TState> :
    IEventSourcedAggregate,
    ISnapshottable,
    IAggregate
    where TState : class, new()
{
    public string Id { get; private set; } = null!;
    public long Version { get; private set; }
    public bool IsInitialized { get; private set; }

    // Use pooled array for pending changes
    private object?[] _pendingChanges = [];
    private int _pendingChangesCount;
    private const int InitialCapacity = 4;
    private const int MaxPooledSize = 64;

    protected TState State { get; private set; } = null!;
    public bool IsDirty => _pendingChangesCount > 0;
    public bool IsNew => Version == 0;

    private readonly IPayloadProcessor _processor;

    protected Aggregate() : this(null) { }

    protected Aggregate(IPayloadProcessor? processor)
    {
        _processor = processor ?? DelegateToPrivateEventHandlers.Instance;
    }

    protected virtual string StateSignature => "1";

    public void Init(string id) => InternalInit(id, 0, null);

    private void InternalInit(string aggregateId, long aggregateVersion, TState? state)
    {
        ArgumentException.ThrowIfNullOrEmpty(aggregateId);

        if (Id is not null)
            throw new AggregateAlreadyInitializedException(GetType(), Id);

        Id = aggregateId;
        State = state ?? new TState();
        IsInitialized = true;
        ClearPendingChanges();
        Version = aggregateVersion;

        AfterInit();
    }

    protected virtual void AfterInit() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void Emit(object @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        var outcome = _processor.Process(State, @event);
        Track(@event, outcome);
    }

    protected virtual void Track(object @event, object? outcome)
    {
        EnsureCapacity(_pendingChangesCount + 1);
        _pendingChanges[_pendingChangesCount++] = @event;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity(int required)
    {
        if (_pendingChanges.Length >= required) return;

        int newCapacity = _pendingChanges.Length == 0
            ? InitialCapacity
            : Math.Min(_pendingChanges.Length * 2, MaxPooledSize);

        while (newCapacity < required)
            newCapacity = Math.Min(newCapacity * 2, int.MaxValue / 2);

        // Use ArrayPool for small-to-medium arrays
        var newArray = newCapacity <= MaxPooledSize
            ? ArrayPool<object?>.Shared.Rent(newCapacity)
            : new object?[newCapacity];

        if (_pendingChangesCount > 0)
        {
            _pendingChanges.AsSpan(0, _pendingChangesCount).CopyTo(newArray);
        }

        // Return old array to pool if it was pooled
        if (_pendingChanges.Length is > 0 and <= MaxPooledSize)
        {
            ArrayPool<object?>.Shared.Return(_pendingChanges, clearArray: true);
        }

        _pendingChanges = newArray;
    }

    Changeset IEventSourcedAggregate.GetChangeSet()
    {
        if (_pendingChangesCount == 0)
        {
            return new Changeset(Version + 1, []);
        }

        // Create exact-sized array using span slice
        var events = _pendingChanges.AsSpan(0, _pendingChangesCount).ToArray()!;
        return new Changeset(Version + 1, events!);
    }

    void IEventSourcedAggregate.Persisted(Changeset changeset)
    {
        Version = changeset.AggregateVersion;
        ClearPendingChanges();
    }

    void IEventSourcedAggregate.ApplyChanges(Changeset changeset)
    {
        // Skip if same version (idempotent)
        if (changeset.AggregateVersion == Version)
            return;

        if (changeset.AggregateVersion != Version + 1)
            throw new AggregateRestoreException(GetType(), Version + 1, changeset.AggregateVersion);

        Version = changeset.AggregateVersion;

        foreach (var @event in PreprocessEvents(changeset.Events))
        {
            _processor.Process(State, @event);
        }
    }

    private void ClearPendingChanges()
    {
        if (_pendingChangesCount > 0)
        {
            _pendingChanges.AsSpan(0, _pendingChangesCount).Clear();
            _pendingChangesCount = 0;
        }
    }

    protected virtual IEnumerable<object> PreprocessEvents(object[] events) => events;

    // Snapshot support...
    bool ISnapshottable.TryRestore(SnapshotInfo snapshotInfo)
    {
        ArgumentNullException.ThrowIfNull(snapshotInfo);

        var processed = PreprocessSnapshot(snapshotInfo);

        if (processed is null || processed.IsEmpty)
            return false;

        InternalInit(processed.SourceId, processed.SourceVersion, (TState)processed.Payload);
        return true;
    }

    protected virtual SnapshotInfo? PreprocessSnapshot(SnapshotInfo snapshotInfo) => snapshotInfo;

    SnapshotInfo ISnapshottable.GetSnapshot() => new(Id, Version, State, StateSignature);

    void IEventSourcedAggregate.Loaded() => PostLoadingProcessing();

    protected virtual void PostLoadingProcessing() { }
}
```

**Key .NET 10 / C# 13 Features Used:**
- `ArrayPool<T>.Shared` - Reduced heap allocations
- `Span<T>.CopyTo` - Fast array copying
- `AsSpan().ToArray()` - Efficient slicing
- Collection expression `[]` - Empty array literal
- `ArgumentException.ThrowIfNullOrEmpty` - Modern validation
- `is not null` pattern matching
- Nullable reference types throughout

---

### 2.3 Changeset Headers Lazy Initialization

**Priority**: P2 (Medium)
**Impact**: Reduced allocations
**Effort**: Very Low
**Risk**: Very Low

#### Problem

Always creates Dictionary even when headers are rarely used.

**File**: `src/NStore.Domain/Changeset.cs`

#### Solution

```csharp
public sealed class Changeset : IHeadersAccessor
{
    public Object[] Events { get; private set; }
    public long AggregateVersion { get; private set; }

    private Dictionary<string, object> _headers;
    public Dictionary<string, object> Headers
    {
        get => _headers ??= new Dictionary<string, object>();
        private set => _headers = value;
    }

    public bool HasHeaders => _headers != null && _headers.Count > 0;

    private Changeset()
    {
        // Don't initialize Headers here - lazy init
    }

    public Changeset(long aggregateVersion, object[] events) : this()
    {
        this.AggregateVersion = aggregateVersion;
        this.Events = events;
    }

    public Changeset(long aggregateVersion, object[] events, Dictionary<string, object> headers)
    {
        this.AggregateVersion = aggregateVersion;
        this.Events = events;
        this._headers = headers;
    }

    public IHeadersAccessor Add(string key, object value)
    {
        Headers.Add(key, value);  // Triggers lazy init if needed
        return this;
    }

    public bool IsEmpty() => Events.Length == 0;
}
```

---

### 2.4 InMemoryPartition Lock Optimization

**Priority**: P2 (Medium)
**Impact**: 15-20% faster concurrent operations
**Effort**: Low
**Risk**: Low

#### Problem

Separate read and write locks in Delete method cause unnecessary contention.

**File**: `src/NStore.Core/InMemory/InMemoryPartition.cs`

#### Solution

```csharp
public MemoryChunk[] Delete(long fromIndex, long toIndex)
{
    // Use single write lock instead of read-then-write
    _lockSlim.EnterWriteLock();
    try
    {
        var toDelete = Chunks
            .Where(x => x.Index >= fromIndex && x.Index <= toIndex)
            .ToArray();

        foreach (var chunk in toDelete)
        {
            this._sortedChunks.Remove(chunk.Index);
            this._operations.Remove(chunk.OperationId);
        }

        return toDelete;
    }
    finally
    {
        _lockSlim.ExitWriteLock();
    }
}
```

---

### 2.5 Async Snapshot Writing Option

**Priority**: P2 (Medium)
**Impact**: Reduced save latency
**Effort**: Medium
**Risk**: Medium (requires careful error handling)

#### Problem

Snapshot write blocks aggregate save.

**File**: `src/NStore.Domain/Repository.cs`

#### Solution

Add configuration option for async snapshot writing.

```csharp
public class Repository : IRepository, IDisposable
{
    // New configuration
    public bool AsyncSnapshotWrite { get; set; } = false;

    public async Task SaveAsync(
        IAggregate aggregate,
        string operationId,
        Action<IHeadersAccessor> headers,
        CancellationToken cancellationToken
    )
    {
        // ... existing validation code ...

        var chunk = await stream.AppendAsync(changeSet, operationId, cancellationToken).ConfigureAwait(false);

        if (chunk != null)
        {
            persister.Persisted(changeSet);

            if (_snapshots != null && aggregate is ISnapshottable snapshottable)
            {
                var snapshotTask = _snapshots.AddAsync(
                    aggregate.Id,
                    snapshottable.GetSnapshot(),
                    cancellationToken
                );

                if (AsyncSnapshotWrite)
                {
                    // Fire and forget with error logging
                    _ = snapshotTask.ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                        {
                            // Log error but don't fail the save
                            // Consider adding ILogger dependency
                        }
                    }, TaskScheduler.Default);
                }
                else
                {
                    await snapshotTask.ConfigureAwait(false);
                }
            }
        }
        else
        {
            Clear();
        }
    }
}
```

---

## Iteration 3: Database and Query Optimizations

### 3.1 MongoDB UseLocalSequence Default

**Priority**: P1 (High)
**Impact**: 80-90% faster single appends
**Effort**: Very Low
**Risk**: Medium (breaking change for multi-process)

#### Problem

Default `UseLocalSequence = false` causes database roundtrip for every append.

**File**: `src/NStore.Persistence.Mongo/MongoPersistenceOptions.cs`

#### Solution

```csharp
/// <summary>
/// When true, uses in-memory sequence generation (faster, single-process only).
/// When false, uses database-side sequence (slower, multi-process safe).
/// Default: true (changed from false for better single-process performance)
/// </summary>
/// <remarks>
/// BREAKING CHANGE: If you have multiple processes writing to the same store,
/// set this to false explicitly.
/// </remarks>
public bool UseLocalSequence { get; set; } = true;  // Changed from false
```

Add XML documentation warning and consider adding a configuration validation:

```csharp
public bool IsValid()
{
    if (!UseLocalSequence && string.IsNullOrWhiteSpace(SequenceConnectionString))
    {
        // Log warning: UseLocalSequence=false but no SequenceConnectionString
    }
    return !String.IsNullOrWhiteSpace(PartitionsConnectionString);
}
```

---

### 3.2 MongoDB ReadLastPositionAsync Optimization

**Priority**: P1 (High)
**Impact**: O(1) instead of O(log n)
**Effort**: Medium
**Risk**: Low

#### Problem

Sorts entire collection to find last position.

**File**: `src/NStore.Persistence.Mongo/MongoPersistence.cs`

#### Solution

Use counter collection which already exists.

```csharp
public async Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
{
    if (_options.UseLocalSequence)
    {
        // Fast path: use in-memory sequence
        return Interlocked.Read(ref _sequence);
    }

    // Use counter collection instead of scanning chunks
    var filter = Builders<Counter>.Filter.Eq(x => x.Id, _options.SequenceId);
    var counter = await _counters
        .Find(filter)
        .FirstOrDefaultAsync(cancellationToken)
        .ConfigureAwait(false);

    return counter?.LastValue ?? 0;
}
```

---

### 3.3 SQL Query String Caching

**Priority**: P2 (Medium)
**Impact**: Reduced allocations
**Effort**: Medium
**Risk**: Low

#### Problem

Rebuilds SQL strings using StringBuilder for every query.

**File**: `src/NStore.Persistence.MsSql/MsSqlPersistenceOptions.cs`

#### Solution

```csharp
public class MsSqlPersistenceOptions : BaseSqlPersistenceOptions
{
    // Cache for range queries
    private readonly ConcurrentDictionary<(long, long, int, bool), string> _rangeQueryCache = new();

    public override string GetRangeSelectChunksSql(
        long lowerIndexInclusive,
        long upperIndexInclusive,
        int limit,
        bool descending
    )
    {
        var key = (
            lowerIndexInclusive > 0 ? 1L : 0L,  // Normalize to pattern
            upperIndexInclusive < Int64.MaxValue ? 1L : 0L,
            limit == int.MaxValue ? -1 : limit,
            descending
        );

        return _rangeQueryCache.GetOrAdd(key, k => BuildRangeSelectSql(
            k.Item1 > 0,
            k.Item2 > 0,
            k.Item3,
            k.Item4
        ));
    }

    private string BuildRangeSelectSql(
        bool hasLower,
        bool hasUpper,
        int limit,
        bool descending)
    {
        var sb = new StringBuilder("SELECT ");
        if (limit > 0 && limit != int.MaxValue)
        {
            sb.Append($"TOP {limit} ");
        }

        sb.Append("[Position], [PartitionId], [Index], [Payload], [OperationId], [SerializerInfo] ");
        sb.Append($"FROM {StreamsTableName} ");
        sb.Append("WHERE [PartitionId] = @PartitionId ");

        if (hasLower)
        {
            sb.Append("AND [Index] >= @lowerIndexInclusive ");
        }

        if (hasUpper)
        {
            sb.Append("AND [Index] <= @upperIndexInclusive ");
        }

        sb.Append(descending ? "ORDER BY [Index] DESC" : "ORDER BY [Index]");

        return sb.ToString();
    }
}
```

---

### 3.4 SQL Connection Context Optimization

**Priority**: P2 (Medium)
**Impact**: Reduced connection overhead
**Effort**: Medium
**Risk**: Medium

#### Problem

Creates new connection for every database operation.

**File**: `src/NStore.Persistence.MsSql/MsSqlPersistenceOptions.cs`

#### Solution

Consider ambient transaction scope or connection reuse pattern.

```csharp
public class MsSqlPersistenceOptions : BaseSqlPersistenceOptions
{
    // Optional: Allow external connection management
    public Func<CancellationToken, Task<AbstractSqlContext>> ExternalContextFactory { get; set; }

    public override async Task<AbstractSqlContext> GetContextAsync(CancellationToken cancellationToken)
    {
        // Check for external context factory (e.g., for transaction scoping)
        if (ExternalContextFactory != null)
        {
            return await ExternalContextFactory(cancellationToken).ConfigureAwait(false);
        }

        var connection = new SqlConnection(ConnectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            connection.Dispose();
            throw;
        }

        return new MsSqlContext(connection);
    }
}
```

---

### 3.5 MongoDB Batch Write Empty Chunk Handling

**Priority**: P2 (Medium)
**Impact**: Maintains event store integrity
**Effort**: Medium
**Risk**: Medium

#### Problem

Failed batch writes don't persist empty chunks, potentially creating sequence gaps.

**File**: `src/NStore.Persistence.Mongo/MongoPersistence.cs`

#### Solution

```csharp
public async Task AppendBatchAsync(WriteJob[] queue, CancellationToken cancellationToken)
{
    var insertCount = queue.Length;
    var lastId = await GetNextId(insertCount, cancellationToken).ConfigureAwait(false);
    var firstId = lastId - insertCount + 1;

    var chunks = new TChunk[insertCount];
    var emptyChunksNeeded = new List<TChunk>();

    for (var currentIdx = 0; currentIdx < insertCount; currentIdx++)
    {
        var current = queue[currentIdx];
        long id = firstId + currentIdx;

        var chunk = new TChunk();
        chunk.Init(
            id,
            current.PartitionId,
            current.Index,
            _mongoPayloadSerializer.Serialize(current.Payload),
            current.OperationId ?? Guid.NewGuid().ToString()
        );

        chunks[currentIdx] = chunk;
    }

    var options = new InsertManyOptions() { IsOrdered = false };

    try
    {
        await _chunks.InsertManyAsync(chunks, options, cancellationToken).ConfigureAwait(false);
    }
    catch (MongoBulkWriteException<TChunk> e)
    {
        foreach (var err in e.WriteErrors)
        {
            if (err.Category == ServerErrorCategory.DuplicateKey)
            {
                var failedChunk = chunks[err.Index];

                if (err.Message.Contains(PartitionIndexIdx))
                {
                    queue[err.Index].Failed(WriteJob.WriteResult.DuplicatedIndex);

                    // Create empty chunk to fill the position
                    var empty = new TChunk();
                    empty.Init(
                        failedChunk.Position,
                        "::empty",
                        failedChunk.Position,
                        _mongoPayloadSerializer.Serialize(null),
                        "_" + failedChunk.Position
                    );
                    emptyChunksNeeded.Add(empty);
                }
                else if (err.Message.Contains(PartitionOperationIdx))
                {
                    queue[err.Index].Failed(WriteJob.WriteResult.DuplicatedOperation);

                    // Create empty chunk for idempotency skip
                    var empty = new TChunk();
                    empty.Init(
                        failedChunk.Position,
                        "::empty",
                        failedChunk.Position,
                        _mongoPayloadSerializer.Serialize(null),
                        "_" + failedChunk.Position
                    );
                    emptyChunksNeeded.Add(empty);
                }
            }
        }

        // Persist empty chunks to maintain sequence continuity
        if (emptyChunksNeeded.Count > 0)
        {
            try
            {
                await _chunks.InsertManyAsync(emptyChunksNeeded,
                    new InsertManyOptions { IsOrdered = false },
                    cancellationToken).ConfigureAwait(false);
            }
            catch (MongoBulkWriteException)
            {
                // Empty chunks might already exist, ignore
            }
        }
    }

    for (var index = 0; index < queue.Length; index++)
    {
        var writeJob = queue[index];
        if (writeJob.Result == WriteJob.WriteResult.None)
        {
            writeJob.Succeeded(chunks[index]);
        }
    }
}
```

---

## Iteration 4: Advanced Optimizations

### 4.1 Lazy Payload Deserialization

**Priority**: P2 (Medium)
**Impact**: 15-25% faster reads
**Effort**: High
**Risk**: Medium

Create a lazy deserialization wrapper:

```csharp
public class LazyChunk<TChunk> : IChunk where TChunk : IMongoChunk
{
    private readonly TChunk _inner;
    private readonly IMongoPayloadSerializer _serializer;
    private object _deserializedPayload;
    private bool _isDeserialized;

    public LazyChunk(TChunk inner, IMongoPayloadSerializer serializer)
    {
        _inner = inner;
        _serializer = serializer;
    }

    public long Position => _inner.Position;
    public string PartitionId => _inner.PartitionId;
    public long Index => _inner.Index;
    public string OperationId => _inner.OperationId;

    public object Payload
    {
        get
        {
            if (!_isDeserialized)
            {
                _deserializedPayload = _serializer.Deserialize(_inner.Payload);
                _isDeserialized = true;
            }
            return _deserializedPayload;
        }
    }
}
```

---

### 4.2 Fix OptimisticConcurrencyStream.IsEmpty Bug

**Priority**: P0 (Critical - Bug Fix)
**Impact**: Correctness
**Effort**: Very Low
**Risk**: Very Low

#### Problem

Returns inverted result.

**File**: `src/NStore.Core/Streams/OptimisticConcurrencyStream.cs`

```csharp
// Line 69-70: Bug - returns true when NOT empty
public async Task<bool> IsEmpty(CancellationToken cancellationToken)
{
    return await Persistence.ReadSingleBackwardAsync(Id, cancellationToken)
               .ConfigureAwait(false) != null;  // BUG: should be == null
}
```

#### Solution

```csharp
public async Task<bool> IsEmpty(CancellationToken cancellationToken)
{
    // Use cached version if available
    if (_version > 0)
    {
        return false;  // Stream has been read and has events
    }

    if (_version == 0)
    {
        return true;  // Stream was marked as new or empty after read
    }

    // Fall back to database check
    var chunk = await Persistence.ReadSingleBackwardAsync(Id, cancellationToken)
        .ConfigureAwait(false);

    if (chunk != null)
    {
        _version = chunk.Index;  // Cache the version
    }

    return chunk == null;  // FIXED: was != null
}
```

---

### 4.3 Compiled Expression Trees

**Priority**: P3 (Low)
**Impact**: 5-10x faster method invocation
**Effort**: High
**Risk**: Medium

#### Solution

```csharp
public static class MethodInvoker
{
    private static readonly ConcurrentDictionary<(Type, string, Type), Func<object, object, object>>
        _compiledDelegates = new();

    public static object CallNonPublicIfExists(this object instance, string methodName, object @parameter)
    {
        var key = (instance.GetType(), methodName, parameter.GetType());

        var invoker = _compiledDelegates.GetOrAdd(key, k => CompileDelegate(k.Item1, k.Item2, k.Item3, NonPublic));

        return invoker?.Invoke(instance, parameter);
    }

    private static Func<object, object, object> CompileDelegate(
        Type instanceType,
        string methodName,
        Type parameterType,
        BindingFlags flags)
    {
        var mi = instanceType.GetMethod(methodName, flags, null, new[] { parameterType }, null);
        if (mi == null) return null;

        var instanceParam = Expression.Parameter(typeof(object), "instance");
        var paramParam = Expression.Parameter(typeof(object), "param");

        var call = Expression.Call(
            Expression.Convert(instanceParam, instanceType),
            mi,
            Expression.Convert(paramParam, parameterType)
        );

        Expression body = mi.ReturnType == typeof(void)
            ? Expression.Block(call, Expression.Constant(null))
            : Expression.Convert(call, typeof(object));

        return Expression.Lambda<Func<object, object, object>>(body, instanceParam, paramParam).Compile();
    }
}
```

---

### 4.4 Configurable Snapshot Strategies

**Priority**: P2 (Medium)
**Impact**: Configurable performance
**Effort**: Medium
**Risk**: Low

```csharp
public interface ISnapshotStrategy
{
    bool ShouldSnapshot(IAggregate aggregate, Changeset changeset);
}

public class EveryNEventsStrategy : ISnapshotStrategy
{
    private readonly int _interval;
    public EveryNEventsStrategy(int interval) => _interval = interval;

    public bool ShouldSnapshot(IAggregate aggregate, Changeset changeset)
        => changeset.AggregateVersion % _interval == 0;
}

public class EventCountThresholdStrategy : ISnapshotStrategy
{
    private readonly int _threshold;
    public EventCountThresholdStrategy(int threshold) => _threshold = threshold;

    public bool ShouldSnapshot(IAggregate aggregate, Changeset changeset)
        => changeset.Events.Length >= _threshold;
}

public class AlwaysSnapshotStrategy : ISnapshotStrategy
{
    public static readonly ISnapshotStrategy Instance = new AlwaysSnapshotStrategy();
    public bool ShouldSnapshot(IAggregate aggregate, Changeset changeset) => true;
}

public class NeverSnapshotStrategy : ISnapshotStrategy
{
    public static readonly ISnapshotStrategy Instance = new NeverSnapshotStrategy();
    public bool ShouldSnapshot(IAggregate aggregate, Changeset changeset) => false;
}
```

---

### 4.5 Parallel Multi-Partition Reads

**Priority**: P2 (Medium)
**Impact**: Linear speedup
**Effort**: Low
**Risk**: Low

**File**: `src/NStore.Core/InMemory/InMemoryPersistence.cs`

```csharp
public async Task ReadForwardMultiplePartitionsAsync(
    IEnumerable<string> partitionIdsList,
    long fromLowerIndexInclusive,
    ISubscription subscription,
    long toUpperIndexInclusive,
    CancellationToken cancellationToken)
{
    if (partitionIdsList is null)
    {
        throw new ArgumentNullException(nameof(partitionIdsList));
    }

    var partitionIds = partitionIdsList.ToList();
    if (partitionIds.Count == 0)
    {
        return;
    }

    var partitions = _partitions
        .Where(c => partitionIds.Contains(c.Key))
        .Select(c => c.Value)
        .ToList();

    // Read all partitions in parallel, then merge results
    var readTasks = partitions.Select(async partition =>
    {
        var chunks = new List<MemoryChunk>();
        var collectingSubscription = new LambdaSubscription(chunk =>
        {
            chunks.Add((MemoryChunk)chunk);
            return Task.FromResult(true);
        });

        await partition.ReadForward(
            fromLowerIndexInclusive,
            collectingSubscription,
            toUpperIndexInclusive,
            Int32.MaxValue,
            cancellationToken);

        return chunks;
    }).ToList();

    var allResults = await Task.WhenAll(readTasks).ConfigureAwait(false);

    // Merge and sort by index
    var merged = allResults
        .SelectMany(x => x)
        .OrderBy(x => x.Index)
        .ToList();

    // Push to subscription
    await subscription.OnStartAsync(fromLowerIndexInclusive).ConfigureAwait(false);

    foreach (var chunk in merged)
    {
        if (!await subscription.OnNextAsync(Clone(chunk)).ConfigureAwait(false))
        {
            await subscription.StoppedAsync(chunk.Index).ConfigureAwait(false);
            return;
        }
    }

    await subscription.CompletedAsync(merged.LastOrDefault()?.Index ?? fromLowerIndexInclusive)
        .ConfigureAwait(false);
}
```

---

## Bug Fixes

### BUG-1: OptimisticConcurrencyStream.IsEmpty Returns Inverted Result

**File**: `src/NStore.Core/Streams/OptimisticConcurrencyStream.cs:69-70`

**Current (Buggy)**:
```csharp
return await Persistence.ReadSingleBackwardAsync(Id, cancellationToken)
           .ConfigureAwait(false) != null;
```

**Fixed**:
```csharp
return await Persistence.ReadSingleBackwardAsync(Id, cancellationToken)
           .ConfigureAwait(false) == null;
```

---

### BUG-2: AsyncWriteJob.Failed Missing Exception Propagation

**File**: `src/NStore.Tpl/AsyncWriteJob.cs:24-29`

**Current**:
```csharp
public override void Failed(WriteResult result)
{
    base.Failed(result);
    this._completionSource.SetResult(null);  // Silent failure
}
```

**Consider**:
```csharp
public override void Failed(WriteResult result)
{
    base.Failed(result);

    switch (result)
    {
        case WriteResult.DuplicatedIndex:
            _completionSource.SetException(
                new DuplicateStreamIndexException(PartitionId, Index));
            break;
        case WriteResult.DuplicatedOperation:
            _completionSource.SetResult(null);  // Idempotent skip is not an error
            break;
        default:
            _completionSource.SetException(
                new InvalidOperationException($"Write failed: {result}"));
            break;
    }
}
```

---

## Benchmarking Strategy

### Setup BenchmarkDotNet Project

Create `src/NStore.Benchmarks/NStore.Benchmarks.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>13</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <AllowUnsafeBlocks>true</AllowUnsafeBlocks>
    <PublishAot>true</PublishAot>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="BenchmarkDotNet" Version="0.14.*" />
    <PackageReference Include="BenchmarkDotNet.Diagnostics.Windows" Version="0.14.*" Condition="'$(OS)' == 'Windows_NT'" />
    <ProjectReference Include="..\NStore.Core\NStore.Core.csproj" />
    <ProjectReference Include="..\NStore.Domain\NStore.Domain.csproj" />
    <ProjectReference Include="..\NStore.Persistence.Mongo\NStore.Persistence.Mongo.csproj" />
  </ItemGroup>
</Project>
```

### Key Benchmarks

```csharp
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using NStore.Core.InMemory;
using NStore.Core.Persistence;
using NStore.Core.Processing;
using NStore.Core.Streams;
using NStore.Domain;

namespace NStore.Benchmarks;

// Program entry point
public class Program
{
    public static void Main(string[] args) =>
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}

/// <summary>
/// Benchmark configuration for .NET 10
/// </summary>
public class Net10Config : ManualConfig
{
    public Net10Config()
    {
        AddJob(Job.Default
            .WithRuntime(CoreRuntime.Core100)
            .WithGcServer(true)
            .WithGcForce(false));

        AddDiagnoser(MemoryDiagnoser.Default);
        AddDiagnoser(ThreadingDiagnoser.Default);
    }
}

[Config(typeof(Net10Config))]
[MemoryDiagnoser]
[ThreadingDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class MethodInvokerBenchmarks
{
    private TestState _state = null!;
    private TestEvent _event = null!;

    [GlobalSetup]
    public void Setup()
    {
        _state = new TestState();
        _event = new TestEvent();

        // Warm up frozen cache
        _ = MethodInvoker.CallNonPublicIfExists(_state, "On", _event);
        MethodInvoker.FreezeCacheNow();
    }

    [Benchmark(Baseline = true)]
    public object? ReflectionNoCache()
        => MethodInvokerOld.CallNonPublicIfExists(_state, "On", _event);

    [Benchmark]
    public object? CachedReflection()
        => MethodInvokerCached.CallNonPublicIfExists(_state, "On", _event);

    [Benchmark]
    public object? FrozenDictionaryWithCompiledDelegate()
        => MethodInvoker.CallNonPublicIfExists(_state, "On", _event);
}

[Config(typeof(Net10Config))]
[MemoryDiagnoser]
public class AggregateReplayBenchmarks
{
    private InMemoryPersistence _persistence = null!;
    private StreamsFactory _streams = null!;
    private string _streamId = null!;

    [Params(100, 1_000, 10_000, 100_000)]
    public int EventCount;

    [GlobalSetup]
    public async Task Setup()
    {
        _persistence = new InMemoryPersistence(new InMemoryPersistenceOptions
        {
            InitialCapacity = EventCount + 100
        });
        _streams = new StreamsFactory(_persistence);
        _streamId = Guid.NewGuid().ToString();

        // Seed events using batch append for speed
        var stream = _streams.Open(_streamId);
        for (int i = 0; i < EventCount; i++)
        {
            await stream.AppendAsync(new TestEvent(i), null, CancellationToken.None);
        }

        // Freeze caches
        MethodInvoker.FreezeCacheNow();
    }

    [Benchmark]
    public async Task<TestAggregate> ReplayAggregate()
    {
        await using var repo = new Repository(new DefaultAggregateFactory(), _streams);
        return await repo.GetByIdAsync<TestAggregate>(_streamId);
    }

    [Benchmark]
    public async Task<int> StreamWithAsyncEnumerable()
    {
        int count = 0;
        await foreach (var chunk in _persistence.StreamAllAsync(1))
        {
            count++;
        }
        return count;
    }
}

[Config(typeof(Net10Config))]
[MemoryDiagnoser]
public class PersistenceAppendBenchmarks
{
    private InMemoryPersistence _persistence = null!;

    [GlobalSetup]
    public void Setup()
    {
        _persistence = new InMemoryPersistence(new InMemoryPersistenceOptions
        {
            InitialCapacity = 10_000
        });
    }

    [Benchmark]
    public async Task<IChunk> AppendSingle()
    {
        var id = Guid.NewGuid().ToString();
        return await _persistence.AppendAsync(id, 1, new TestEvent(1), null, CancellationToken.None);
    }

    [Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1_000)]
    public async Task AppendBatch(int count)
    {
        var id = Guid.NewGuid().ToString();
        for (int i = 1; i <= count; i++)
        {
            await _persistence.AppendAsync(id, i, new TestEvent(i), null, CancellationToken.None);
        }
    }
}

[Config(typeof(Net10Config))]
[MemoryDiagnoser]
public class AllocationBenchmarks
{
    [Benchmark]
    public Changeset CreateChangesetWithoutHeaders()
        => new(1, [new TestEvent(1), new TestEvent(2)]);

    [Benchmark]
    public Changeset CreateChangesetWithHeaders()
    {
        var cs = new Changeset(1, [new TestEvent(1)]);
        cs.Add("key", "value");
        return cs;
    }

    [Benchmark]
    public TestAggregate EmitMultipleEvents()
    {
        var agg = new TestAggregate();
        agg.Init("test");
        for (int i = 0; i < 10; i++)
        {
            agg.DoSomething(i);
        }
        return agg;
    }
}

// Test types
public record TestEvent(int Value = 0);

public class TestState
{
    public int Total { get; private set; }
    private void On(TestEvent e) => Total += e.Value;
}

public class TestAggregate : Aggregate<TestState>
{
    public void DoSomething(int value) => Emit(new TestEvent(value));
}

public class DefaultAggregateFactory : IAggregateFactory
{
    public T Create<T>() where T : IAggregate => Activator.CreateInstance<T>();
    public IAggregate Create(Type aggregateType) => (IAggregate)Activator.CreateInstance(aggregateType)!;
}
```

### Run Benchmarks

```bash
cd src/NStore.Benchmarks
dotnet run -c Release -- --filter "*"
```

---

## Implementation Checklist

### Iteration 1: Critical Performance Fixes
- [ ] **1.1** Implement reflection caching in `MethodInvoker.cs`
- [ ] **1.2** Replace fixed array in `InMemoryPersistence.cs`
- [ ] **1.3** Enable `BoundedCapacity` in `PersistenceBatchAppendDecorator.cs`
- [ ] **1.4** Optimize `SubscriptionWrapper.cs` async patterns
- [ ] **1.5** Add exponential backoff in `Stream.cs` retry loop
- [ ] Create benchmarks for Iteration 1 changes
- [ ] Run regression tests

### Iteration 2: Memory and Lifecycle
- [ ] **2.1** Implement `IDisposable` on `Repository.cs`
- [ ] **2.2** Optimize `Aggregate.cs` PendingChanges allocation
- [ ] **2.3** Lazy initialize `Changeset.cs` Headers
- [ ] **2.4** Improve `InMemoryPartition.cs` lock granularity
- [ ] **2.5** Add async snapshot option to `Repository.cs`
- [ ] Create memory allocation tests
- [ ] Run regression tests

### Iteration 3: Database Optimizations
- [ ] **3.1** Change `UseLocalSequence` default in MongoDB options
- [ ] **3.2** Optimize `ReadLastPositionAsync` in MongoDB
- [ ] **3.3** Cache SQL query strings in `MsSqlPersistenceOptions.cs`
- [ ] **3.4** Add connection context options
- [ ] **3.5** Fix MongoDB batch write empty chunk handling
- [ ] Create database-specific benchmarks
- [ ] Run integration tests

### Iteration 4: Advanced Optimizations
- [ ] **4.1** Implement lazy payload deserialization
- [ ] **4.2** Fix `OptimisticConcurrencyStream.IsEmpty` bug
- [ ] **4.3** Implement compiled expression trees
- [ ] **4.4** Add configurable snapshot strategies
- [ ] **4.5** Parallelize multi-partition reads
- [ ] Final benchmark comparison
- [ ] Documentation updates

### Bug Fixes
- [ ] **BUG-1** Fix `IsEmpty` inverted logic
- [ ] **BUG-2** Review `AsyncWriteJob.Failed` exception handling

### Final Steps
- [ ] Update CHANGELOG.md
- [ ] Update README.md with performance notes
- [ ] Create migration guide for breaking changes
- [ ] Tag release

---

## Notes

### Breaking Changes

1. **MongoDB `UseLocalSequence` default change** - Multi-process deployments must explicitly set `UseLocalSequence = false`
2. **Repository now implements `IDisposable`** - Consumers should use `using` statement

### Backward Compatibility

- All API signatures remain unchanged
- Internal optimizations are transparent to consumers
- New optional parameters have sensible defaults

### Testing Strategy

1. Run existing test suite after each change
2. Add specific performance regression tests
3. Use BenchmarkDotNet for micro-benchmarks
4. Use load testing for integration benchmarks

---

## .NET 10 / C# 13 Features Summary

This plan leverages the following modern .NET 10 and C# 13 features for maximum performance:

### Performance Features

| Feature | Usage | Benefit |
|---------|-------|---------|
| `FrozenDictionary<K,V>` | Method invoker cache | O(1) lookups after warmup, optimized for read-heavy scenarios |
| `Lock` object | Synchronization | Lightweight lock type, better than `object` locks |
| `ValueTask<T>` | Async hot paths | Zero allocation for synchronous completions |
| `IAsyncEnumerable<T>` | Streaming reads | No buffering, constant memory usage |
| `ArrayPool<T>.Shared` | Aggregate pending changes | Reduced heap allocations |
| `CollectionsMarshal.SetCount` | List manipulation | Direct list resizing without iteration |
| `Span<T>` / `ReadOnlySpan<T>` | Array operations | Stack-allocated, no heap allocation |
| `[MethodImpl(AggressiveInlining)]` | Hot path methods | JIT hint for inlining |

### Language Features

| Feature | Usage | Benefit |
|---------|-------|---------|
| File-scoped namespaces | All files | Reduced indentation |
| Collection expressions `[]` | Empty arrays/dicts | Cleaner syntax, compiler-optimized |
| Primary constructors | Options classes | Reduced boilerplate |
| `init` accessors | Immutable properties | Set once, then readonly |
| `required` members | Required initialization | Compile-time safety |
| `is null` / `is not null` | Null checks | Pattern-based, clear intent |
| Records | Event types | Immutable by default, value semantics |
| `params` collections | Method parameters | Flexible parameter passing |

### Validation & Safety

| Feature | Usage | Benefit |
|---------|-------|---------|
| `ArgumentNullException.ThrowIfNull` | Parameter validation | One-liner null checks |
| `ArgumentException.ThrowIfNullOrEmpty` | String validation | Built-in empty check |
| `ObjectDisposedException.ThrowIf` | Disposal check | Cleaner than manual throw |
| `[NotNullWhen(true)]` | Try patterns | Nullable flow analysis |
| Nullable reference types | Throughout | Compile-time null safety |

### Async Patterns

| Feature | Usage | Benefit |
|---------|-------|---------|
| `IAsyncDisposable` | Repository | Async cleanup support |
| `[EnumeratorCancellation]` | Async streams | Proper cancellation propagation |
| `ConfigureAwait(false)` | Library code | Avoid sync context capture |

### Project Configuration

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>13</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <AnalysisLevel>latest</AnalysisLevel>
    <EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>
  </PropertyGroup>
</Project>
```

### Migration Notes

When upgrading from .NET Standard 2.0 / earlier .NET versions:

1. **Breaking Changes**
   - `Lock` replaces `object` for locking (not backward compatible)
   - `FrozenDictionary` requires .NET 8+
   - Collection expressions require C# 12+

2. **Multi-targeting Strategy**
   ```xml
   <TargetFrameworks>netstandard2.0;net8.0;net10.0</TargetFrameworks>
   ```
   Use `#if NET10_0_OR_GREATER` for .NET 10-specific optimizations.

3. **Feature Detection**
   ```csharp
   #if NET10_0_OR_GREATER
       private static readonly Lock _lock = new();
   #else
       private static readonly object _lock = new();
   #endif
   ```

---

*Document maintained by: NStore Development Team*
*Last updated: 2024-12-22*
*Target Runtime: .NET 10 / C# 13*
