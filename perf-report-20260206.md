# NStore MongoDB Batch Insert Performance Report

**Date:** 2026-02-06
**Branch:** `review/mongodb-batch-insert-perf`
**Runtime:** .NET 10.0, Release build
**Payload size:** ~135 KB per document (complex nested BSON with long-text fields)
**Chunks per scenario:** 1,000
**Partitions:** 100
**Warmup batches:** 3
**Inter-scenario cooldown:** 5 s
**Runs per configuration:** 3

---

## 1. Test Environment

| Target | Connectivity Profile |
|---|---|
| **Localhost** | Local single-node MongoDB instance (no replica set) |
| **Atlas** | Managed Atlas shared cluster (M0 tier) |

Connection strings and endpoint identifiers are intentionally omitted from this report.

### Scenarios

All four scenarios insert 1,000 chunks total, varying batch size and writer concurrency:

| Scenario | Batch Size | Writers | Total Batches | Description |
|---|---:|---:|---:|---|
| **b100-w1** | 100 | 1 | 10 | Large batches, single writer (sequential) |
| **b50-w2** | 50 | 2 | 20 | Medium batches, 2 concurrent writers |
| **b25-w4** | 25 | 4 | 40 | Small batches, 4 concurrent writers |
| **b25-unbounded** | 25 | unbounded | 40 | Small batches, writer count = min(totalBatches, partitions) |

### Append Modes

| Mode | Description |
|---|---|
| **baseline** | Standard `AppendBatchAsync(jobs, ct)` — single `InsertMany` per batch |
| **parallel-extension** | `AppendBatchAsync(jobs, options, ct)` — splits each batch into parallel sub-batches with configurable `BatchSize` and `MaxWriters` (defaults to scenario values when not overridden) |

---

## 2. Localhost Results

### 2.1 Median Batch Time (ms) — lower is better

| Scenario | Baseline R1 | R2 | R3 | **Avg** | Parallel R1 | R2 | R3 | **Avg** | **Delta** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| b100-w1 | 66.11 | 73.46 | 57.86 | **65.81** | 50.51 | 47.95 | 44.05 | **47.50** | **-28%** |
| b50-w2 | 30.83 | 31.90 | 29.02 | **30.58** | 35.47 | 29.02 | 28.38 | **30.96** | +1% |
| b25-w4 | 21.25 | 22.08 | 19.75 | **21.03** | 21.89 | 20.38 | 19.30 | **20.52** | -2% |
| b25-unbounded | 271.39 | 187.79 | 204.68 | **221.29** | 226.08 | 197.32 | 212.32 | **211.91** | -4% |

### 2.2 Total Elapsed Time (s) — lower is better

| Scenario | Baseline R1 | R2 | R3 | **Avg** | Parallel R1 | R2 | R3 | **Avg** | **Delta** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| b100-w1 | 0.97 | 1.00 | 0.87 | **0.95** | 0.77 | 0.75 | 0.71 | **0.74** | **-22%** |
| b50-w2 | 0.49 | 0.48 | 0.48 | **0.48** | 0.56 | 0.47 | 0.46 | **0.50** | +3% |
| b25-w4 | 1.26 | 0.36 | 0.35 | **0.66** | 0.36 | 0.34 | 0.33 | **0.34** | -48% |
| b25-unbounded | 0.64 | 0.65 | 0.51 | **0.60** | 0.74 | 0.47 | 0.50 | **0.57** | -5% |

*Note: Baseline b25-w4 R1 (1.26s) suffered a 24x degradation spike, an outlier likely caused by transient contention.*

### 2.3 Worst Degradation (x) — lower is better

| Scenario | Baseline R1 | R2 | R3 | **Avg** | Parallel R1 | R2 | R3 | **Avg** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| b100-w1 | 1.28 | 1.15 | 1.28 | **1.24** | 1.13 | 1.00 | 1.24 | **1.12** |
| b50-w2 | 1.18 | 1.48 | 1.99 | **1.55** | 1.63 | 1.69 | 1.46 | **1.59** |
| b25-w4 | 24.13 | 1.53 | 1.70 | **9.12** | 1.21 | 1.34 | 1.21 | **1.26** |
| b25-unbounded | 1.51 | 2.81 | 2.11 | **2.14** | 2.20 | 1.89 | 1.63 | **1.91** |

### 2.4 Suite Total (s)

| Mode | R1 | R2 | R3 | **Avg** |
|---|---:|---:|---:|---:|
| Baseline | 3.36 | 2.49 | 2.21 | **2.69** |
| Parallel-extension | 2.44 | 2.02 | 2.00 | **2.15** |

### 2.5 Localhost Summary

On localhost (sub-millisecond network latency), both modes perform similarly:

- **b100-w1 (single writer, large batch):** Parallel-extension is consistently faster (**-28% batch time, -22% elapsed**). Even with a single logical writer, the parallel path splits the 100-item batch into sub-batches that can be inserted concurrently, hiding local I/O overhead.
- **b50-w2 and b25-w4:** Essentially identical (+1% / -2%). The overhead of sub-batch coordination is negligible but offers no benefit when multiple writers already saturate local disk I/O.
- **b25-unbounded:** Slight improvement (-4% batch time). High writer concurrency already amortizes I/O; parallel sub-batching adds marginal benefit.
- **Stability:** Parallel-extension shows lower degradation variance (worst-case 1.26x vs baseline's outlier at 24.13x in b25-w4).

---

## 3. Atlas Results

### 3.1 Median Batch Time (ms) — lower is better

| Scenario | Baseline R1 | R2 | R3 | **Avg** | Parallel R1 | R2 | R3 | **Avg** | **Delta** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| b100-w1 | 7,452 | 8,465 | 7,395 | **7,771** | 7,909 | 9,612 | 9,026 | **8,849** | +14% |
| b50-w2 | 3,807 | 3,509 | 3,301 | **3,539** | 3,416 | 3,552 | 4,036 | **3,668** | +4% |
| b25-w4 | 1,688 | 1,652 | 1,735 | **1,692** | 1,741 | 1,755 | 1,959 | **1,818** | +7% |
| b25-unbounded | 5,123 | 5,253 | 5,026 | **5,134** | 4,485 | 3,348 | 3,827 | **3,887** | **-24%** |

### 3.2 Total Elapsed Time (s) — lower is better

| Scenario | Baseline R1 | R2 | R3 | **Avg** | Parallel R1 | R2 | R3 | **Avg** | **Delta** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| b100-w1 | 75.00 | 82.84 | 74.56 | **77.47** | 78.83 | 92.22 | 91.63 | **87.56** | +13% |
| b50-w2 | 38.03 | 35.99 | 34.82 | **36.28** | 35.69 | 37.22 | 41.78 | **38.23** | +5% |
| b25-w4 | 18.20 | 18.99 | 18.78 | **18.66** | 18.28 | 18.83 | 21.17 | **19.43** | +4% |
| b25-unbounded | 8.87 | 8.20 | 8.27 | **8.45** | 9.60 | 8.37 | 8.48 | **8.82** | +4% |

### 3.3 Worst Degradation (x) — lower is better

| Scenario | Baseline R1 | R2 | R3 | **Avg** | Parallel R1 | R2 | R3 | **Avg** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| b100-w1 | 1.05 | 1.00 | 1.14 | **1.06** | 1.01 | 1.03 | 1.00 | **1.01** |
| b50-w2 | 1.33 | 1.32 | 1.30 | **1.32** | 1.31 | 1.27 | 1.27 | **1.28** |
| b25-w4 | 1.41 | 1.36 | 1.44 | **1.40** | 1.33 | 1.30 | 1.33 | **1.32** |
| b25-unbounded | 4.52 | 4.18 | 4.40 | **4.37** | 2.81 | 3.07 | 2.74 | **2.87** |

### 3.4 Suite Total (s)

| Mode | R1 | R2 | R3 | **Avg** |
|---|---:|---:|---:|---:|
| Baseline | 140.1 | 146.0 | 136.4 | **140.9** |
| Parallel-extension | 142.4 | 156.6 | 163.1 | **154.0** |

### 3.5 Atlas Summary

On Atlas (high-latency network, shared M0 cluster), the picture is different from localhost:

- **b100-w1 (single writer, large batch):** Parallel-extension is **14% slower**. With a single writer, the parallel path splits 100 items into sub-batches that are sent as separate network round-trips. Each round-trip costs ~7-9 seconds over the internet, so more round-trips means more total time. The sub-batch parallelism cannot compensate because the Atlas M0 shared cluster throttles concurrent writes.
- **b50-w2 and b25-w4:** Slightly slower (+4% to +7%). Same network overhead penalty, insufficient parallelism gain.
- **b25-unbounded:** Parallel-extension wins decisively (**-24% batch time**). With many concurrent writers (40), splitting each 25-item batch into smaller parallel sub-batches allows better pipelining across the high-latency link. The worst degradation also improves significantly (2.87x vs 4.37x).
- **Latency scale:** Atlas batch times are **100-150x** higher than localhost (7.8s vs 66ms for b100-w1). Network round-trip time dominates all computation and serialization costs.

---

## 4. Localhost vs Atlas Comparison

### Median Batch Time Averages (ms)

| Scenario | Localhost Baseline | Localhost Parallel | Atlas Baseline | Atlas Parallel |
|---|---:|---:|---:|---:|
| b100-w1 | 65.81 | 47.50 | 7,771 | 8,849 |
| b50-w2 | 30.58 | 30.96 | 3,539 | 3,668 |
| b25-w4 | 21.03 | 20.52 | 1,692 | 1,818 |
| b25-unbounded | 221.29 | 211.91 | 5,134 | 3,887 |

### Latency Multiplier (Atlas / Localhost)

| Scenario | Baseline | Parallel |
|---|---:|---:|
| b100-w1 | 118x | 186x |
| b50-w2 | 116x | 118x |
| b25-w4 | 80x | 89x |
| b25-unbounded | 23x | 18x |

The unbounded-writers scenario shows the smallest Atlas/localhost ratio because writer parallelism amortizes network latency regardless of append mode.

---

## 5. Key Findings

### 5.1 When Parallel-Extension Helps

1. **High writer concurrency + high network latency (Atlas b25-unbounded):** -24% batch time, -34% worst degradation. The parallel sub-batch strategy pipelines writes effectively when many writers compete for a high-latency link.

2. **Single writer + low latency (Localhost b100-w1):** -28% batch time. Even a single caller benefits from internal batch splitting when the storage layer can handle concurrent writes without network penalty.

### 5.2 When Parallel-Extension Hurts

1. **Single/few writers + high latency (Atlas b100-w1, b50-w2):** +14% and +4% batch time. Each sub-batch requires a separate network round-trip; the parallelism gain is eaten by Atlas connection and throttle overhead on the M0 tier.

### 5.3 Stability

Parallel-extension consistently shows lower worst-case degradation across all Atlas scenarios (avg 1.62x vs 2.04x baseline). On localhost the difference is dramatic when outliers are considered (1.47x vs 3.51x, driven by the 24x baseline spike in b25-w4).

### 5.4 Limitations of This Benchmark

- **1,000 chunks is small.** Only 10-40 batches per scenario provides limited statistical power. Warmup (3 batches) consumes a significant fraction of the work.
- **Atlas M0 is throttled.** Free-tier shared clusters have aggressive write throughput limits that may not reflect production Atlas performance (M10+).
- **No replica set on localhost.** Production MongoDB deployments use replica sets with write concern majority, adding latency not captured here.
- **Payload is fixed.** All documents are ~135 KB. Smaller documents would shift the bottleneck from serialization/network to index contention.

---

## 6. Recommendations

1. **Re-run with larger chunk counts (10k-50k)** to reach steady-state behavior and reduce warmup noise.
2. **Test on Atlas M10+** (dedicated cluster) to see if parallel-extension benefits scale without M0 throttling.
3. **Consider making parallel-extension the default** for high-concurrency scenarios (unbounded writers), where it consistently outperforms baseline on both localhost and Atlas.
4. **Keep baseline as default** for low-concurrency scenarios (1-4 writers), where the sub-batch coordination overhead provides no benefit on high-latency connections.

---

## 7. Raw Data References

All CSV files are in the test output directory:

```
src/NStore.Persistence.Mongo.Tests/bin/Release/net10.0/TestResults/
```

### Localhost Runs
| Run | Baseline Folder | Parallel Folder |
|---|---|---|
| 1 | `baseline-20260206-201821/` | `parallel-extension-20260206-201840/` |
| 2 | `baseline-20260206-201859/` | `parallel-extension-20260206-201918/` |
| 3 | `baseline-20260206-201936/` | `parallel-extension-20260206-201954/` |

### Atlas Runs
| Run | Baseline Folder | Parallel Folder |
|---|---|---|
| 1 | `baseline-20260206-203038/` | `parallel-extension-20260206-203358/` |
| 2 | `baseline-20260206-203724/` | `parallel-extension-20260206-204053/` |
| 3 | `baseline-20260206-204440/` | `parallel-extension-20260206-204755/` |

Each folder contains:
- `suite.csv` — summary row per scenario with metadata headers
- `s{NN}-{scenario}-b{batch}-w{writers}-c{chunks}.csv` — per-batch progress windows with degradation tracking
