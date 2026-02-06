# Code Review: `mongodb_batch_insert_performance_tests`

**File:** `src/NStore.Persistence.Mongo.Tests/MongoBatchInsertPerformanceTests.cs`
**Date:** 2026-02-06
**Branch:** `feature/perfs`

---

## Overview

The `mongodb_batch_insert_performance_tests` class implements a multi-scenario batch insert benchmark for the Mongo persistence layer. It supports configurable parallelism (bounded and unbounded writers), reservoir-sampled latency tracking, per-window degradation detection, and CSV log output at both suite and scenario level.

The overall structure is solid: partition-aware writer isolation, channel-based result aggregation, and deterministic payload generation are well-designed. However, the review identified **2 correctness bugs**, **1 data-loss risk**, **2 misleading-output issues**, **2 code smells**, and **3 minor issues**.

---

## Findings

### 1. [BUG] Workers not awaited before persistence disposal

**Severity:** High
**Lines:** 330-462

When a worker task throws an exception, the following sequence occurs:

1. `Task.WhenAll(workerTasks)` propagates the error.
2. `batchResults.Writer.TryComplete(completionError)` completes the channel with the error.
3. The `await foreach` reader loop (line 348) throws when it encounters the error.
4. Execution jumps to the `finally` block (line 457), skipping `await workersCompletionTask` (line 410).
5. The persistence is disposed while other workers may still be executing `AppendBatchAsync`.

```csharp
// Line 410 — SKIPPED if the reader loop throws
await workersCompletionTask.ConfigureAwait(false);

totalStopwatch.Stop();
// ...
finally
{
    // Line 458 — disposes while workers may still be running
    if (persistence is IDisposable disposablePersistence)
    {
        disposablePersistence.Dispose();
    }
}
```

**Impact:** Workers writing to a disposed `MongoPersistence` instance will produce secondary exceptions. These exceptions are observed (via `Task.WhenAll`) so they won't cause `UnobservedTaskException`, but the MongoDB driver may leave connections in an undefined state, and error output becomes noisy and misleading.

**Recommendation:** Always await `workersCompletionTask` before disposing the persistence:

```csharp
finally
{
    // Ensure all workers have stopped before disposing shared resources
    try
    {
        await workersCompletionTask.ConfigureAwait(false);
    }
    catch
    {
        // Already propagated via the channel
    }

    if (persistence is IAsyncDisposable asyncDisposable)
        await asyncDisposable.DisposeAsync().ConfigureAwait(false);
    else if (persistence is IDisposable disposable)
        disposable.Dispose();
}
```

---

### 2. [BUG] `maxDegradation` accepts values below 1.0

**Severity:** Medium
**Lines:** 1065-1087 (`ReadDoubleSetting`)

The validation for `NSTORE_MONGO_BATCH_PERF_MAX_DEGRADATION` only checks `value <= 0`:

```csharp
if (value <= 0)
{
    throw new ArgumentOutOfRangeException(
        variable,
        $"{variable} must be > 0, but was {value}.");
}
```

A degradation multiplier of `1.0x` means "no degradation". Values below `1.0` (e.g., `0.5x`) would assert that performance *improved* by 2x — almost certainly a configuration mistake. Setting `maxDegradation=0.3` would silently pass unless the last batch window happened to be more than 3.3x faster than the first.

**Recommendation:** Validate `value >= 1.0`:

```csharp
if (value < 1.0)
{
    throw new ArgumentOutOfRangeException(
        variable,
        $"{variable} must be >= 1.0 (1.0 = no degradation), but was {value}.");
}
```

---

### 3. [DATA LOSS] Scenario log not flushed between progress windows

**Severity:** Medium
**Lines:** 399-401

The scenario-level `StreamWriter` writes progress lines without flushing:

```csharp
_output.WriteLine(line);
await writer.WriteLineAsync(line);  // line 400
// No FlushAsync() here
```

The suite-level writer correctly flushes after each scenario entry (lines 173-174), but the per-scenario detail log only flushes once at the very end (line 430). If the process crashes or is killed during a long-running scenario, all accumulated progress data for that scenario is lost.

For a benchmark that may run for minutes or hours, this is a significant observability gap.

**Recommendation:** Add `await writer.FlushAsync();` after line 400, or set `AutoFlush = true` on the `StreamWriter`.

---

### 4. [MISLEADING] Suite CSV reports configured writers instead of effective writers

**Severity:** Medium
**Lines:** 443, 155-170

The `perf_run_result` records `scenario.Writers` (the configured value):

```csharp
WriterCount = scenario.Writers, // line 443 — reports 0 for "unbounded"
```

For unbounded scenarios where `Writers=0`, the suite CSV column shows `writers=0`. The actual parallelism level is computed at line 217 as `effectiveWriterCount = Math.Max(1, Math.Min(totalBatches, partitionCount))`, which could be, for example, 100 — but this information is only logged to xUnit output, not captured in the CSV.

When comparing results across runs, the CSV is the primary artifact. Having `writers=0` makes it impossible to correlate parallelism with throughput from the CSV alone.

**Recommendation:** Add `EffectiveWriterCount` to `perf_run_result` and include it in both the scenario and suite CSV output:

```csharp
// In perf_run_result:
public int EffectiveWriterCount { get; set; }

// In RunScenarioAsync:
EffectiveWriterCount = effectiveWriterCount,
```

---

### 5. [MISLEADING] Degradation baseline doesn't account for concurrent warmup

**Severity:** Low-Medium
**Lines:** 260-268, 373-376

The warmup phase runs batches **sequentially** — one at a time, round-robin across writer states:

```csharp
for (var i = 0; i < warmupBatches; i++)
{
    var writerState = writerStates[i % effectiveWriterCount];
    var warmupJobs = CreateJobs(nextId, scenario.BatchSize, writerState);
    // ...
    await batcher.AppendBatchAsync(warmupJobs, CancellationToken.None).ConfigureAwait(false);
}
```

The main run then switches to fully concurrent writers. The degradation baseline is captured from the first measurement window of the concurrent run:

```csharp
if (!baselineAvgBatchMs.HasValue)
{
    baselineAvgBatchMs = windowAvgBatchMs;
}
```

This first concurrent window may include:
- Thread pool ramp-up latency
- MongoDB connection pool expansion
- WiredTiger cache warm-up under concurrent load
- .NET `Channel<T>` consumer/producer coordination overhead

If this first window is slow, the baseline is inflated and subsequent degradation appears artificially low. Conversely, if the first window benefits from OS/driver buffering, the baseline is artificially fast, triggering false degradation alarms.

**Recommendation:** Run warmup batches concurrently (using the same worker pattern as the main run), or skip the first N concurrent windows before capturing the baseline.

---

### 6. [CODE SMELL] Sync-over-async `.Wait()` in `Create(true)`

**Severity:** Low
**File:** `MongoPersistenceFixture.cs:78`

```csharp
_mongoPersistence.InitAsync(CancellationToken.None).Wait();
```

Called from `RunScenarioAsync` (an async method) via `Create(true)`. This blocks the calling thread synchronously. In a test context without a `SynchronizationContext`, this usually works, but it wastes a thread pool thread during initialization — which is ironic for a performance benchmark.

**Recommendation:** Add an async overload (`CreateAsync`) or use `await` at the call site.

---

### 7. [CODE SMELL] Only checks `IDisposable`, not `IAsyncDisposable`

**Severity:** Low
**Lines:** 458-462

```csharp
if (persistence is IDisposable disposablePersistence)
{
    disposablePersistence.Dispose();
}
```

In an async method, failing to check for `IAsyncDisposable` may leave async resources (MongoDB connections, cursors, sessions) improperly cleaned up. The synchronous `Dispose()` path in many MongoDB driver types only does partial cleanup.

**Recommendation:** Prefer `IAsyncDisposable` when available (see fix in Finding #1).

---

### 8. [MINOR] `CsvEscape` doesn't handle newlines

**Severity:** Low
**Lines:** 735-748

```csharp
if (!value.Contains(",") && !value.Contains("\""))
{
    return value;
}
```

Per RFC 4180, fields containing newlines (`\n`, `\r`) must also be quoted. While the current inputs (scenario names, file paths) are unlikely to contain newlines, this is a correctness gap that could produce malformed CSV if inputs change.

**Recommendation:** Add `!value.Contains('\n') && !value.Contains('\r')` to the guard condition.

---

### 9. [MINOR] `Percentile` allocates and sorts on every call

**Severity:** Low
**Lines:** 652-658

```csharp
var ordered = values.OrderBy(x => x).ToArray();
```

Called at each progress interval with up to 4,096 samples. LINQ `OrderBy` allocates an intermediate buffer and performs O(n log n) sorting. For a benchmark, this adds GC pressure and CPU overhead during measurement windows.

**Recommendation:** Use an in-place sort on a copied array, or use a selection algorithm (e.g., Quickselect) for O(n) median/percentile computation.

---

### 10. [MINOR] `Create(true)` silently overwrites base class instance fields

**Severity:** Low
**Files:** `MongoPersistenceFixture.cs:68-81`, `PersistenceFixture.cs:40-54`

Each `Create(true)` call overwrites `_mongoPersistence`, `_mongoConnectionString`, and `_options` on the base class. The perf test calls it once per scenario in a loop. After the first scenario disposes the local `persistence` variable, `_mongoPersistence` still points to the now-disposed instance. The second `Create(true)` overwrites it.

This doesn't cause issues today because `BasePersistenceTest.Dispose()` only checks `_persistence` (which is `null` due to `autoCreateStore=false`). But if the base class adds cleanup logic referencing `_mongoPersistence`, it will break silently.

**Recommendation:** Either avoid mutating shared base-class state from the perf test, or override `Dispose(bool)` to handle the multi-persistence lifecycle.

---

## Summary Table

| # | Severity | Category | Description |
|---|----------|----------|-------------|
| 1 | High | Bug | Workers not awaited before persistence disposal |
| 2 | Medium | Bug | `maxDegradation` accepts values below 1.0 |
| 3 | Medium | Data loss | Scenario log not flushed between progress windows |
| 4 | Medium | Misleading | Suite CSV reports `writers=0` for unbounded scenarios |
| 5 | Low-Med | Misleading | Degradation baseline set from un-warmed concurrent path |
| 6 | Low | Code smell | Sync-over-async `.Wait()` in `Create(true)` |
| 7 | Low | Code smell | No `IAsyncDisposable` check in async context |
| 8 | Low | Minor | `CsvEscape` missing newline handling |
| 9 | Low | Minor | `Percentile` allocates and sorts on every call |
| 10 | Low | Minor | `Create(true)` overwrites shared base-class fields |

---

## What Works Well

- **Partition-aware writer isolation:** Each writer gets a disjoint partition range via `CreateWriterStates`, preventing cross-writer index collisions. Stream indices within each partition increase monotonically.
- **Reservoir sampling:** The `AddDurationSample` implementation correctly uses Algorithm R with bounded memory (4,096 samples), with independent reservoirs for per-window and overall statistics.
- **Channel-based aggregation:** Using `Channel<T>` with `SingleReader=true` is the right pattern for fan-in from multiple writers to a single progress reporter.
- **Deterministic payloads:** Fixed `Random` seeds and template-based text generation make results reproducible across runs.
- **Scenario isolation:** Each scenario gets a fresh persistence via `Create(true)` with `DropOnInit=true`, and inter-scenario cooldown (5s) reduces MongoDB oplog pressure bleed-through.
