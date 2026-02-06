using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using NStore.Core.Persistence;
using NStore.Persistence.Tests;
using Xunit;
using Xunit.Abstractions;

namespace NStore.Persistence.Mongo.Tests
{
    // ReSharper disable once InconsistentNaming
    public class mongodb_batch_insert_performance_tests : BasePersistenceTest
    {
        private const string PerfEnabledVar = "NSTORE_MONGO_BATCH_PERF";
        private const string TotalChunksVar = "NSTORE_MONGO_BATCH_PERF_TOTAL_CHUNKS";
        private const string BatchSizeVar = "NSTORE_MONGO_BATCH_PERF_BATCH_SIZE";
        private const string WritersVar = "NSTORE_MONGO_BATCH_PERF_WRITERS";
        private const string PartitionCountVar = "NSTORE_MONGO_BATCH_PERF_PARTITIONS";
        private const string ProgressEveryBatchesVar = "NSTORE_MONGO_BATCH_PERF_PROGRESS_EVERY_BATCHES";
        private const string WarmupBatchesVar = "NSTORE_MONGO_BATCH_PERF_WARMUP";
        private const string MaxDegradationVar = "NSTORE_MONGO_BATCH_PERF_MAX_DEGRADATION";
        private const string ScenarioFilterVar = "NSTORE_MONGO_BATCH_PERF_SCENARIO";
        private const string LogFileVar = "NSTORE_MONGO_BATCH_PERF_LOG_FILE";
        private const string AppendModeVar = "NSTORE_MONGO_BATCH_PERF_APPEND_MODE";
        private const string ParallelBatchSizeVar = "NSTORE_MONGO_BATCH_PERF_PARALLEL_BATCH_SIZE";
        private const string ParallelWritersVar = "NSTORE_MONGO_BATCH_PERF_PARALLEL_WRITERS";
        private const string PerformanceProfilesConfigPath = "NStore:Mongo:Performance:TestParameters";
        private const string MongoConnectionEnvVar = "NSTORE_MONGODB";
        private const int InterScenarioDelaySeconds = 5;
        private static readonly string[] PerfMongoConnectionConfigKeys =
        {
            "NStore:Mongo:Performance:ConnectionString",
            "NStore:Mongo:Performance:AtlasConnectionString"
        };

        private static readonly string[] MongoConnectionConfigKeys =
        {
            "NStore:Mongo:ConnectionString"
        };

        private const int DurationSampleLimit = 4_096;
        private const string LongTextTemplate =
            "This benchmark payload is intentionally verbose to stress serialization, indexing, and storage paths with deterministic long-form content that can be reproduced across runs and compared over time for regression analysis.";

        private readonly ITestOutputHelper _output;

        public mongodb_batch_insert_performance_tests(ITestOutputHelper output) : base(false)
        {
            _output = output;
        }

        [Fact]
        [Trait("Category", "Performance")]
        public async Task should_measure_batch_insert_performance_degradation()
        {
            if (!IsEnabled(Environment.GetEnvironmentVariable(PerfEnabledVar)))
            {
                _output.WriteLine($"Skipping: set {PerfEnabledVar}=1 to run this test.");
                return;
            }

            var partitionCount = ReadIntSetting(PartitionCountVar, 100, 2);
            var warmupBatches = ReadIntSetting(WarmupBatchesVar, 3, 0);
            var maxDegradation = ReadDoubleSetting(MaxDegradationVar);
            var configuredProgressEveryBatches = ReadOptionalIntSetting(ProgressEveryBatchesVar, 1);
            var scenarios = ReadConfiguredScenarios();
            if (scenarios.Count == 0)
            {
                scenarios = new List<perf_test_configuration>
                {
                    ReadSingleScenarioFromEnvironment()
                };
            }

            scenarios = scenarios
                .OrderBy(x => x.Writers <= 0 ? int.MaxValue : x.Writers)
                .ThenBy(x => x.BatchSize)
                .ThenBy(x => x.Name)
                .ToList();

            var scenarioFilter = Environment.GetEnvironmentVariable(ScenarioFilterVar);
            if (!string.IsNullOrWhiteSpace(scenarioFilter))
            {
                var selectedNames = scenarioFilter
                    .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Trim())
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .ToArray();

                scenarios = scenarios
                    .Where(s => selectedNames.Any(name =>
                        s.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                    .ToList();

                if (scenarios.Count == 0)
                {
                    throw new ArgumentException(
                        $"{ScenarioFilterVar} did not match any configured scenario name in {PerformanceProfilesConfigPath}.");
                }
            }

            for (var i = 0; i < scenarios.Count; i++)
            {
                if (scenarios[i].Writers > 0 && scenarios[i].Writers > partitionCount)
                {
                    throw new ArgumentOutOfRangeException(
                        WritersVar,
                        $"Scenario '{scenarios[i].Name}' has writers={scenarios[i].Writers}, but {PartitionCountVar} is {partitionCount}. Writers must be <= partitions.");
                }
            }

            var suiteLogFile = ResolveSuiteLogFilePath(Environment.GetEnvironmentVariable(LogFileVar));
            var suiteDirectory = Path.GetDirectoryName(suiteLogFile);
            if (!string.IsNullOrWhiteSpace(suiteDirectory))
            {
                Directory.CreateDirectory(suiteDirectory);
            }

            var mongoTargetUrl = ResolveSanitizedMongoTargetUrl();
            _output.WriteLine($"Mongo batch benchmark suite: scenarios={scenarios.Count}, partitions={partitionCount}, warmup={warmupBatches}.");
            _output.WriteLine($"Mongo batch benchmark target: {mongoTargetUrl}");
            _output.WriteLine($"Mongo batch benchmark suite log: {suiteLogFile}");
            Console.WriteLine($"Mongo batch benchmark target: {mongoTargetUrl}");

            await using var suiteWriter = new StreamWriter(suiteLogFile, append: false);
            await suiteWriter.WriteLineAsync($"# mongodb_url={mongoTargetUrl}");
            await suiteWriter.WriteLineAsync($"# started_utc={DateTimeOffset.UtcNow:O}");
            await suiteWriter.WriteLineAsync(
                $"# scenarios={scenarios.Count},partitions={partitionCount},warmup_batches={warmupBatches},progress_every_batches={(configuredProgressEveryBatches.HasValue ? configuredProgressEveryBatches.Value.ToString(CultureInfo.InvariantCulture) : "auto")},inter_scenario_delay_s={InterScenarioDelaySeconds}");
            await suiteWriter.WriteLineAsync(
                "scenario_name,batch_size,writers,total_chunks,partitions,total_batches,total_elapsed_s,overall_throughput_items_per_sec,median_single_batch_ms,representative_payload_size_bytes,last_degradation_x,worst_degradation_x,scenario_log_file");
            await suiteWriter.FlushAsync();

            var runResults = new List<perf_run_result>(scenarios.Count);
            for (var scenarioIndex = 0; scenarioIndex < scenarios.Count; scenarioIndex++)
            {
                var scenario = scenarios[scenarioIndex];
                var scenarioLogFile = ResolveScenarioLogFilePath(suiteLogFile, scenario, scenarioIndex + 1);
                var result = await RunScenarioAsync(
                    scenario,
                    partitionCount,
                    warmupBatches,
                    configuredProgressEveryBatches,
                    maxDegradation,
                    mongoTargetUrl,
                    scenarioLogFile).ConfigureAwait(false);

                runResults.Add(result);
                var suiteLine = string.Format(
                    CultureInfo.InvariantCulture,
                    "{0},{1},{2},{3},{4},{5},{6:F2},{7:F0},{8:F2},{9:F0},{10:F3},{11:F3},{12}",
                    CsvEscape(result.ScenarioName),
                    result.BatchSize,
                    result.WriterCount,
                    result.TotalChunks,
                    result.PartitionCount,
                    result.TotalBatches,
                    result.TotalElapsedSeconds,
                    result.OverallThroughputItemsPerSecond,
                    result.MedianSingleBatchMs,
                    result.RepresentativePayloadSizeBytes,
                    result.LastDegradation,
                    result.WorstDegradation,
                    CsvEscape(result.ScenarioLogFile));

                _output.WriteLine(suiteLine);
                await suiteWriter.WriteLineAsync(suiteLine);
                await suiteWriter.FlushAsync();

                if (scenarioIndex + 1 < scenarios.Count)
                {
                    var cooldownMessage =
                        $"Mongo batch benchmark cooldown: waiting {InterScenarioDelaySeconds}s before next scenario to reduce server oplog pressure.";
                    _output.WriteLine(cooldownMessage);
                    Console.WriteLine(cooldownMessage);
                    await Task.Delay(TimeSpan.FromSeconds(InterScenarioDelaySeconds)).ConfigureAwait(false);
                }
            }

            var totalSuiteElapsedSeconds = runResults.Sum(x => x.TotalElapsedSeconds);
            var bestThroughput = runResults.Count == 0 ? 0 : runResults.Max(x => x.OverallThroughputItemsPerSecond);
            var slowestMedianBatchMs = runResults.Count == 0 ? 0 : runResults.Max(x => x.MedianSingleBatchMs);
            var summaryLine = string.Format(
                CultureInfo.InvariantCulture,
                "# summary,scenario_count={0},suite_total_elapsed_s={1:F2},best_overall_throughput_items_per_sec={2:F0},slowest_median_single_batch_ms={3:F2}",
                runResults.Count,
                totalSuiteElapsedSeconds,
                bestThroughput,
                slowestMedianBatchMs);

            _output.WriteLine(summaryLine);
            await suiteWriter.WriteLineAsync(summaryLine);
            await suiteWriter.FlushAsync();
        }

        private async Task<perf_run_result> RunScenarioAsync(
            perf_test_configuration scenario,
            int partitionCount,
            int warmupBatches,
            int? configuredProgressEveryBatches,
            double? maxDegradation,
            string mongoTargetUrl,
            string logFile)
        {
            var totalBatches = scenario.TotalChunks / scenario.BatchSize;
            if (scenario.TotalChunks % scenario.BatchSize != 0)
            {
                totalBatches++;
            }

            var effectiveWriterCount = ResolveEffectiveWriterCount(
                scenario.Writers,
                totalBatches,
                partitionCount);
            var appendMode = ReadAppendModeSetting(AppendModeVar);
            var parallelBatchSize = ReadOptionalIntSetting(ParallelBatchSizeVar, 1) ?? scenario.BatchSize;
            var parallelWriters = ReadOptionalIntSetting(ParallelWritersVar, 1) ?? effectiveWriterCount;
            var progressEveryBatches = configuredProgressEveryBatches.HasValue
                ? configuredProgressEveryBatches.Value
                : Math.Max(1L, totalBatches / 20);
            progressEveryBatches = Math.Min(progressEveryBatches, totalBatches);
            var representativePayloadSizeBytes = EstimatePayloadSizeBytes(
                CreateComplexPayload(1, "perf-stream-0000", 1));
            var appendModeLabel = appendMode == append_mode.parallel_extension
                ? $"parallel-extension(parallel_batch_size={parallelBatchSize},parallel_writers={parallelWriters})"
                : "baseline";

            var logDirectory = Path.GetDirectoryName(logFile);
            if (!string.IsNullOrWhiteSpace(logDirectory))
            {
                Directory.CreateDirectory(logDirectory);
            }

            await using var writer = new StreamWriter(logFile, append: false);
            await writer.WriteLineAsync($"# mongodb_url={mongoTargetUrl}");
            await writer.WriteLineAsync($"# started_utc={DateTimeOffset.UtcNow:O}");
            await writer.WriteLineAsync(
                $"# scenario={scenario.Name},append_mode={appendModeLabel},total_chunks={scenario.TotalChunks},batch_size={scenario.BatchSize},writers={FormatWriters(scenario.Writers)},effective_writers={effectiveWriterCount},partitions={partitionCount},total_batches={totalBatches},warmup_batches={warmupBatches},progress_every_batches={progressEveryBatches}");
            await writer.WriteLineAsync(
                "timestamp_utc,batch,inserted_total,total_chunks,progress_pct,window_batches,window_avg_batch_ms,window_p95_batch_ms,window_throughput_items_per_sec,total_elapsed_s,degradation_x,last_position");
            await writer.FlushAsync();

            _output.WriteLine(
                $"Mongo batch scenario: name={scenario.Name}, appendMode={appendModeLabel}, totalChunks={scenario.TotalChunks}, batchSize={scenario.BatchSize}, writers={FormatWriters(scenario.Writers)}, effectiveWriters={effectiveWriterCount}, partitions={partitionCount}, totalBatches={totalBatches}, warmup={warmupBatches}, progressEveryBatches={progressEveryBatches}.");
            _output.WriteLine($"Mongo batch scenario target: {mongoTargetUrl}");
            _output.WriteLine($"Mongo batch scenario log: {logFile}");
            Console.WriteLine($"Mongo batch scenario target: {mongoTargetUrl}");

            var persistence = Create(true);
            CancellationTokenSource workerCancellation = null;
            Task workersCompletionTask = Task.CompletedTask;
            try
            {
                if (!(persistence is IEnhancedPersistence batcher))
                {
                    throw new InvalidOperationException("Persistence does not expose AppendBatchAsync.");
                }

                var parallelAppendOptions = new ParallelBatchAppendOptions
                {
                    BatchSize = parallelBatchSize,
                    MaxWriters = parallelWriters
                };

                Task AppendJobsAsync(WriteJob[] jobs, CancellationToken ct)
                {
                    if (appendMode == append_mode.parallel_extension)
                    {
                        return batcher.AppendBatchAsync(jobs, parallelAppendOptions, ct);
                    }

                    return batcher.AppendBatchAsync(jobs, ct);
                }

                var writerStates = CreateWriterStates(effectiveWriterCount, partitionCount);
                long nextId = 0;
                long warmupInserted = 0;
                for (var i = 0; i < warmupBatches; i++)
                {
                    var writerState = writerStates[i % effectiveWriterCount];
                    var warmupJobs = CreateJobs(nextId, scenario.BatchSize, writerState);
                    nextId += scenario.BatchSize;
                    warmupInserted += scenario.BatchSize;

                    await AppendJobsAsync(warmupJobs, CancellationToken.None).ConfigureAwait(false);
                    Assert.All(warmupJobs, job => Assert.Equal(WriteJob.WriteResult.Committed, job.Result));
                }

                var totalStopwatch = Stopwatch.StartNew();
                var windowStopwatch = Stopwatch.StartNew();
                var windowDurationSamples = new List<double>(DurationSampleLimit);
                var overallDurationSamples = new List<double>(DurationSampleLimit);
                var windowSampler = new Random(17);
                var overallSampler = new Random(31);
                long windowSampleSeen = 0;
                long overallSampleSeen = 0;
                long windowBatchCount = 0;
                double windowBatchDurationSumMs = 0;

                long insertedTotal = 0;
                long windowInserted = 0;
                double? baselineAvgBatchMs = null;
                double worstDegradation = 0;
                double lastDegradation = 0;

                var batchResults = Channel.CreateUnbounded<batch_execution_result>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false
                });

                long nextBatchNumber = 0;
                var nextPayloadId = nextId;
                workerCancellation = new CancellationTokenSource();
                var workerCancellationToken = workerCancellation.Token;

                var workerTasks = writerStates
                    .Select(writerState =>
                        Task.Run(async () =>
                        {
                            while (!workerCancellationToken.IsCancellationRequested)
                            {
                                var batchNumber = Interlocked.Increment(ref nextBatchNumber);
                                if (batchNumber > totalBatches)
                                {
                                    break;
                                }

                                var remainingForBatch = scenario.TotalChunks - (long)(batchNumber - 1) * scenario.BatchSize;
                                var currentBatchSize = (int)Math.Min(remainingForBatch, scenario.BatchSize);
                                var startId = Interlocked.Add(ref nextPayloadId, currentBatchSize) - currentBatchSize;
                                var jobs = CreateJobs(startId, currentBatchSize, writerState);

                                var batchStopwatch = Stopwatch.StartNew();
                                await AppendJobsAsync(jobs, workerCancellationToken).ConfigureAwait(false);
                                batchStopwatch.Stop();
                                Assert.All(jobs, job => Assert.Equal(WriteJob.WriteResult.Committed, job.Result));

                                await batchResults.Writer.WriteAsync(
                                        new batch_execution_result(
                                            currentBatchSize,
                                            batchStopwatch.Elapsed.TotalMilliseconds),
                                        workerCancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }, workerCancellationToken))
                    .ToArray();

                workersCompletionTask = Task.Run(async () =>
                {
                    Exception completionError = null;
                    try
                    {
                        await Task.WhenAll(workerTasks).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (!IsCancellationException(ex) || !workerCancellationToken.IsCancellationRequested)
                        {
                            completionError = ex;
                        }
                    }
                    finally
                    {
                        batchResults.Writer.TryComplete(completionError);
                    }
                });

                long completedBatches = 0;
                await foreach (var result in batchResults.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    completedBatches++;
                    insertedTotal += result.BatchSize;
                    windowInserted += result.BatchSize;
                    windowBatchCount++;
                    windowBatchDurationSumMs += result.BatchDurationMs;
                    AddDurationSample(windowDurationSamples, ref windowSampleSeen, result.BatchDurationMs, windowSampler);
                    AddDurationSample(overallDurationSamples, ref overallSampleSeen, result.BatchDurationMs, overallSampler);

                    var shouldLogProgress = completedBatches % progressEveryBatches == 0 || insertedTotal >= scenario.TotalChunks;
                    if (!shouldLogProgress)
                    {
                        continue;
                    }

                    var windowAvgBatchMs = windowBatchDurationSumMs / Math.Max(windowBatchCount, 1);
                    var windowP95BatchMs = windowDurationSamples.Count == 0
                        ? 0
                        : Percentile(windowDurationSamples, 0.95);
                    var elapsedWindowSeconds = Math.Max(windowStopwatch.Elapsed.TotalSeconds, double.Epsilon);
                    var windowThroughput = windowInserted / elapsedWindowSeconds;
                    var elapsedTotalSeconds = totalStopwatch.Elapsed.TotalSeconds;
                    var progressPct = insertedTotal * 100d / scenario.TotalChunks;

                    if (!baselineAvgBatchMs.HasValue)
                    {
                        baselineAvgBatchMs = windowAvgBatchMs;
                    }

                    var degradation = windowAvgBatchMs / baselineAvgBatchMs.Value;
                    worstDegradation = Math.Max(worstDegradation, degradation);
                    lastDegradation = degradation;

                    var lastPosition = warmupInserted + insertedTotal;
                    var line = string.Format(
                        CultureInfo.InvariantCulture,
                        "{0},{1},{2},{3},{4:F2},{5},{6:F2},{7:F2},{8:F0},{9:F2},{10:F3},{11}",
                        DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
                        completedBatches,
                        insertedTotal,
                        scenario.TotalChunks,
                        progressPct,
                        windowBatchCount,
                        windowAvgBatchMs,
                        windowP95BatchMs,
                        windowThroughput,
                        elapsedTotalSeconds,
                        degradation,
                        lastPosition);

                    _output.WriteLine(line);
                    await writer.WriteLineAsync(line);

                    windowDurationSamples.Clear();
                    windowSampleSeen = 0;
                    windowBatchCount = 0;
                    windowBatchDurationSumMs = 0;
                    windowInserted = 0;
                    windowStopwatch.Restart();
                }

                await workersCompletionTask.ConfigureAwait(false);

                totalStopwatch.Stop();
                var medianSingleBatchMs = overallDurationSamples.Count == 0
                    ? 0
                    : Percentile(overallDurationSamples, 0.50);

                var summaryLine = string.Format(
                    CultureInfo.InvariantCulture,
                    "# summary,total_chunks={0},total_elapsed_s={1:F2},median_single_batch_ms={2:F2},representative_payload_size_bytes={3:F0},last_degradation_x={4:F3},worst_degradation_x={5:F3}",
                    scenario.TotalChunks,
                    totalStopwatch.Elapsed.TotalSeconds,
                    medianSingleBatchMs,
                    representativePayloadSizeBytes,
                    lastDegradation,
                    worstDegradation);

                _output.WriteLine(summaryLine);
                _output.WriteLine($"Mongo batch scenario log: {logFile}");
                await writer.WriteLineAsync(summaryLine);
                await writer.FlushAsync();

                if (maxDegradation.HasValue)
                {
                    Assert.True(
                        worstDegradation <= maxDegradation.Value,
                        $"Scenario '{scenario.Name}' measured degradation {worstDegradation:F3}x exceeds limit {maxDegradation.Value:F3}x.");
                }

                return new perf_run_result
                {
                    ScenarioName = scenario.Name,
                    BatchSize = scenario.BatchSize,
                    WriterCount = scenario.Writers,
                    TotalChunks = scenario.TotalChunks,
                    PartitionCount = partitionCount,
                    TotalBatches = totalBatches,
                    TotalElapsedSeconds = totalStopwatch.Elapsed.TotalSeconds,
                    OverallThroughputItemsPerSecond = scenario.TotalChunks / Math.Max(totalStopwatch.Elapsed.TotalSeconds, double.Epsilon),
                    MedianSingleBatchMs = medianSingleBatchMs,
                    RepresentativePayloadSizeBytes = representativePayloadSizeBytes,
                    LastDegradation = lastDegradation,
                    WorstDegradation = worstDegradation,
                    ScenarioLogFile = logFile
                };
            }
            finally
            {
                if (workerCancellation != null)
                {
                    workerCancellation.Cancel();
                }

                await workersCompletionTask.ConfigureAwait(false);
                workerCancellation?.Dispose();

                if (persistence is IDisposable disposablePersistence)
                {
                    disposablePersistence.Dispose();
                }
            }
        }

        private static WriteJob[] CreateJobs(
            long startId,
            int batchSize,
            writer_partition_state writerState)
        {
            var jobs = new WriteJob[batchSize];
            for (var i = 0; i < batchSize; i++)
            {
                var currentId = startId + i + 1;
                var partitionOffset = (int)(writerState.NextOffset % writerState.PartitionCount);
                var partitionNumber = writerState.PartitionStart + partitionOffset;
                var partitionId = $"perf-stream-{partitionNumber:D4}";
                var streamIndex = writerState.NextOffset / writerState.PartitionCount + 1;
                writerState.NextOffset++;
                var payload = CreateComplexPayload(currentId, partitionId, streamIndex);
                jobs[i] = new WriteJob(
                    partitionId,
                    streamIndex,
                    payload,
                    null);
            }

            return jobs;
        }

        private static writer_partition_state[] CreateWriterStates(int writerCount, int partitionCount)
        {
            var writerStates = new writer_partition_state[writerCount];
            var basePartitionsPerWriter = partitionCount / writerCount;
            var remainder = partitionCount % writerCount;
            var nextPartitionStart = 0;

            for (var writer = 0; writer < writerCount; writer++)
            {
                var assignedPartitions = basePartitionsPerWriter + (writer < remainder ? 1 : 0);
                writerStates[writer] = new writer_partition_state(nextPartitionStart, assignedPartitions);
                nextPartitionStart += assignedPartitions;
            }

            return writerStates;
        }

        private static int EstimatePayloadSizeBytes(complex_batch_document payload)
        {
            return payload.ToBson().Length;
        }

        private static complex_batch_document CreateComplexPayload(
            long currentId,
            string partitionId,
            long streamIndex)
        {
            var now = DateTime.UtcNow;
            var nestedDocuments = Enumerable.Range(1, 5)
                .Select(i => new nested_document
                {
                    NestedId = BuildLongText("nested-id", currentId, streamIndex, i, 0),
                    Order = i,
                    Active = i % 2 == 0,
                    Score = currentId * 0.001 + i,
                    NestedTitle = BuildLongText("nested-title", currentId, streamIndex, i, 0),
                    NestedSummary = BuildLongText("nested-summary", currentId, streamIndex, i, 0),
                    NestedDescription = BuildLongText("nested-description", currentId, streamIndex, i, 0),
                    BusinessContext = BuildLongText("nested-business-context", currentId, streamIndex, i, 0),
                    OperationalContext = BuildLongText("nested-operational-context", currentId, streamIndex, i, 0),
                    QualityContext = BuildLongText("nested-quality-context", currentId, streamIndex, i, 0),
                    ComplianceContext = BuildLongText("nested-compliance-context", currentId, streamIndex, i, 0),
                    SecurityContext = BuildLongText("nested-security-context", currentId, streamIndex, i, 0),
                    OwnershipContext = BuildLongText("nested-ownership-context", currentId, streamIndex, i, 0),
                    TraceContext = BuildLongText("nested-trace-context", currentId, streamIndex, i, 0),
                    Metrics = new nested_metrics_document
                    {
                        Count = (int)(currentId % 1000) + i,
                        Ratio = i / 5d,
                        Labels = new[]
                        {
                            BuildLongText("metrics-label-a", currentId, streamIndex, i, 0),
                            BuildLongText("metrics-label-b", currentId, streamIndex, i, 0),
                            BuildLongText("metrics-label-c", currentId, streamIndex, i, 0)
                        },
                        MetricOverview = BuildLongText("metrics-overview", currentId, streamIndex, i, 0),
                        MetricWindowDefinition = BuildLongText("metrics-window-definition", currentId, streamIndex, i, 0),
                        MetricComputationNotes = BuildLongText("metrics-computation-notes", currentId, streamIndex, i, 0),
                        MetricDataSource = BuildLongText("metrics-data-source", currentId, streamIndex, i, 0),
                        MetricConfidenceExplanation = BuildLongText("metrics-confidence-explanation", currentId, streamIndex, i, 0),
                        MetricNormalizationRule = BuildLongText("metrics-normalization-rule", currentId, streamIndex, i, 0),
                        MetricAlertPolicy = BuildLongText("metrics-alert-policy", currentId, streamIndex, i, 0),
                        MetricSamplingPlan = BuildLongText("metrics-sampling-plan", currentId, streamIndex, i, 0),
                        MetricAuditNote = BuildLongText("metrics-audit-note", currentId, streamIndex, i, 0),
                        MetricTrace = BuildLongText("metrics-trace", currentId, streamIndex, i, 0)
                    },
                    Children = new[]
                    {
                        new child_document
                        {
                            ChildId = BuildLongText("child-id", currentId, streamIndex, i, 1),
                            Kind = "alpha",
                            Value = i * 10,
                            ChildTitle = BuildLongText("child-title", currentId, streamIndex, i, 1),
                            ChildSummary = BuildLongText("child-summary", currentId, streamIndex, i, 1),
                            ChildDescription = BuildLongText("child-description", currentId, streamIndex, i, 1),
                            ChildContext = BuildLongText("child-context", currentId, streamIndex, i, 1),
                            ChildLifecycle = BuildLongText("child-lifecycle", currentId, streamIndex, i, 1),
                            ChildCompliance = BuildLongText("child-compliance", currentId, streamIndex, i, 1),
                            ChildSecurity = BuildLongText("child-security", currentId, streamIndex, i, 1),
                            ChildOwnership = BuildLongText("child-ownership", currentId, streamIndex, i, 1),
                            ChildProcessing = BuildLongText("child-processing", currentId, streamIndex, i, 1),
                            ChildAudit = BuildLongText("child-audit", currentId, streamIndex, i, 1)
                        },
                        new child_document
                        {
                            ChildId = BuildLongText("child-id", currentId, streamIndex, i, 2),
                            Kind = "beta",
                            Value = i * 10 + 1,
                            ChildTitle = BuildLongText("child-title", currentId, streamIndex, i, 2),
                            ChildSummary = BuildLongText("child-summary", currentId, streamIndex, i, 2),
                            ChildDescription = BuildLongText("child-description", currentId, streamIndex, i, 2),
                            ChildContext = BuildLongText("child-context", currentId, streamIndex, i, 2),
                            ChildLifecycle = BuildLongText("child-lifecycle", currentId, streamIndex, i, 2),
                            ChildCompliance = BuildLongText("child-compliance", currentId, streamIndex, i, 2),
                            ChildSecurity = BuildLongText("child-security", currentId, streamIndex, i, 2),
                            ChildOwnership = BuildLongText("child-ownership", currentId, streamIndex, i, 2),
                            ChildProcessing = BuildLongText("child-processing", currentId, streamIndex, i, 2),
                            ChildAudit = BuildLongText("child-audit", currentId, streamIndex, i, 2)
                        }
                    }
                })
                .ToArray();

            return new complex_batch_document
            {
                DocumentId = currentId,
                StreamKey = partitionId,
                StreamIndex = streamIndex,
                CreatedAtUtc = now,
                DocumentTitle = BuildLongText("root-title", currentId, streamIndex, 0, 0),
                DocumentSummary = BuildLongText("root-summary", currentId, streamIndex, 0, 0),
                DocumentDescription = BuildLongText("root-description", currentId, streamIndex, 0, 0),
                DomainNarrative = BuildLongText("root-domain-narrative", currentId, streamIndex, 0, 0),
                LifecycleNarrative = BuildLongText("root-lifecycle-narrative", currentId, streamIndex, 0, 0),
                ComplianceNarrative = BuildLongText("root-compliance-narrative", currentId, streamIndex, 0, 0),
                SecurityNarrative = BuildLongText("root-security-narrative", currentId, streamIndex, 0, 0),
                OwnershipNarrative = BuildLongText("root-ownership-narrative", currentId, streamIndex, 0, 0),
                ProcessNarrative = BuildLongText("root-process-narrative", currentId, streamIndex, 0, 0),
                AuditNarrative = BuildLongText("root-audit-narrative", currentId, streamIndex, 0, 0),
                Metadata = new metadata_document
                {
                    Tenant = BuildLongText("metadata-tenant", currentId, streamIndex, 0, 0),
                    Region = BuildLongText("metadata-region", currentId, streamIndex, 0, 0),
                    Source = BuildLongText("metadata-source", currentId, streamIndex, 0, 0),
                    Classification = BuildLongText("metadata-classification", currentId, streamIndex, 0, 0),
                    GovernanceModel = BuildLongText("metadata-governance-model", currentId, streamIndex, 0, 0),
                    RetentionPolicy = BuildLongText("metadata-retention-policy", currentId, streamIndex, 0, 0),
                    ProcessingProfile = BuildLongText("metadata-processing-profile", currentId, streamIndex, 0, 0),
                    SecurityProfile = BuildLongText("metadata-security-profile", currentId, streamIndex, 0, 0),
                    SupportModel = BuildLongText("metadata-support-model", currentId, streamIndex, 0, 0),
                    ChangeLog = BuildLongText("metadata-change-log", currentId, streamIndex, 0, 0),
                    Notes = BuildLongText("metadata-notes", currentId, streamIndex, 0, 0),
                    Flags = new[]
                    {
                        BuildLongText("metadata-flag-a", currentId, streamIndex, 0, 0),
                        BuildLongText("metadata-flag-b", currentId, streamIndex, 0, 0),
                        BuildLongText("metadata-flag-c", currentId, streamIndex, 0, 0)
                    }
                },
                NestedDocuments = nestedDocuments
            };
        }

        private static string BuildLongText(
            string fieldName,
            long currentId,
            long streamIndex,
            int nestedOrder,
            int childOrder)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "{0} | doc={1} | stream_index={2} | nested={3} | child={4} | {5} {5}",
                fieldName,
                currentId,
                streamIndex,
                nestedOrder,
                childOrder,
                LongTextTemplate);
        }

        private static double Percentile(IReadOnlyList<double> values, double percentile)
        {
            var ordered = values.OrderBy(x => x).ToArray();
            var index = (int)Math.Ceiling(percentile * ordered.Length) - 1;
            index = Math.Max(0, Math.Min(index, ordered.Length - 1));
            return ordered[index];
        }

        private static void AddDurationSample(
            List<double> samples,
            ref long valuesSeen,
            double value,
            Random random)
        {
            valuesSeen++;
            if (samples.Count < DurationSampleLimit)
            {
                samples.Add(value);
                return;
            }

            var replacement = random.NextInt64(valuesSeen);
            if (replacement < DurationSampleLimit)
            {
                samples[(int)replacement] = value;
            }
        }

        private static bool IsCancellationException(Exception exception)
        {
            if (exception is OperationCanceledException)
            {
                return true;
            }

            if (exception is AggregateException aggregateException)
            {
                return aggregateException
                    .Flatten()
                    .InnerExceptions
                    .All(x => x is OperationCanceledException);
            }

            return false;
        }

        private static string ResolveSuiteLogFilePath(string configuredPath)
        {
            if (!string.IsNullOrWhiteSpace(configuredPath))
            {
                return Path.GetFullPath(configuredPath);
            }

            var fileName = $"mongo-batch-perf-suite-{DateTime.UtcNow:yyyyMMdd-HHmmss}.csv";
            return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "TestResults", fileName));
        }

        private static string ResolveScenarioLogFilePath(
            string suiteLogFile,
            perf_test_configuration scenario,
            int scenarioIndex)
        {
            var directory = Path.GetDirectoryName(suiteLogFile);
            if (string.IsNullOrWhiteSpace(directory))
            {
                directory = Directory.GetCurrentDirectory();
            }

            var suiteFileName = Path.GetFileNameWithoutExtension(suiteLogFile);
            var scenarioName = SanitizeFileName(scenario.Name);
            var fileName = string.Format(
                CultureInfo.InvariantCulture,
                "{0}-s{1:D2}-{2}-b{3}-w{4}-c{5}.csv",
                suiteFileName,
                scenarioIndex,
                scenarioName,
                scenario.BatchSize,
                scenario.Writers,
                scenario.TotalChunks);

            return Path.GetFullPath(Path.Combine(directory, fileName));
        }

        private static string SanitizeFileName(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return "scenario";
            }

            var invalid = Path.GetInvalidFileNameChars();
            var normalized = input
                .Trim()
                .ToLowerInvariant()
                .Select(c => invalid.Contains(c) || char.IsWhiteSpace(c) ? '-' : c)
                .ToArray();

            var sanitized = new string(normalized).Trim('-');
            return string.IsNullOrWhiteSpace(sanitized) ? "scenario" : sanitized;
        }

        private static string CsvEscape(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return string.Empty;
            }

            if (!value.Contains(",") &&
                !value.Contains("\"") &&
                !value.Contains('\n') &&
                !value.Contains('\r'))
            {
                return value;
            }

            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }

        private static string ResolveSanitizedMongoTargetUrl()
        {
            var connectionString = ResolveMongoConnectionStringForReporting();
            try
            {
                var url = new MongoUrl(connectionString);
                var isSrv = connectionString.StartsWith("mongodb+srv://", StringComparison.OrdinalIgnoreCase);
                var protocol = isSrv ? "mongodb+srv" : "mongodb";
                var hosts = string.Join(
                    ",",
                    url.Servers.Select(server => isSrv ? server.Host : $"{server.Host}:{server.Port}"));
                var databaseName = string.IsNullOrWhiteSpace(url.DatabaseName)
                    ? "(default)"
                    : url.DatabaseName;

                return $"{protocol}://{hosts}/{databaseName}";
            }
            catch
            {
                return "unparseable://(invalid-connection-string)";
            }
        }

        private static string ResolveMongoConnectionStringForReporting()
        {
            var mongo = Environment.GetEnvironmentVariable(MongoConnectionEnvVar);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            var config = GetTestConfiguration();
            if (IsEnabled(Environment.GetEnvironmentVariable(PerfEnabledVar)))
            {
                var perfMongo = ReadFirstConfiguredValue(config, PerfMongoConnectionConfigKeys);
                if (!string.IsNullOrWhiteSpace(perfMongo))
                {
                    return perfMongo;
                }
            }

            mongo = ReadFirstConfiguredValue(config, MongoConnectionConfigKeys);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            throw new TestMisconfiguredException(
                $"Mongo connection string not set. Configure {MongoConnectionEnvVar} or appsettings/user-secrets keys: {string.Join(", ", PerfMongoConnectionConfigKeys)} or {string.Join(", ", MongoConnectionConfigKeys)}.");
        }

        private static string ReadFirstConfiguredValue(IConfiguration config, string[] keys)
        {
            for (var i = 0; i < keys.Length; i++)
            {
                var value = config[keys[i]];
                if (!string.IsNullOrWhiteSpace(value))
                {
                    return value;
                }
            }

            return null;
        }

        private static int ResolveEffectiveWriterCount(
            int configuredWriters,
            long totalBatches,
            int partitionCount)
        {
            if (configuredWriters > 0)
            {
                return configuredWriters;
            }

            return Math.Max(1, (int)Math.Min(totalBatches, partitionCount));
        }

        private static string FormatWriters(int configuredWriters)
        {
            return configuredWriters <= 0
                ? "unbounded"
                : configuredWriters.ToString(CultureInfo.InvariantCulture);
        }

        private static List<perf_test_configuration> ReadConfiguredScenarios()
        {
            var config = GetTestConfiguration();
            var scenarioSections = config.GetSection(PerformanceProfilesConfigPath).GetChildren().ToArray();
            if (scenarioSections.Length == 0)
            {
                return new List<perf_test_configuration>();
            }

            var scenarios = new List<perf_test_configuration>(scenarioSections.Length);
            for (var i = 0; i < scenarioSections.Length; i++)
            {
                var scenarioSection = scenarioSections[i];
                var rawName = scenarioSection["Name"];
                var name = string.IsNullOrWhiteSpace(rawName)
                    ? $"scenario-{i + 1}"
                    : rawName.Trim();

                scenarios.Add(new perf_test_configuration
                {
                    Name = name,
                    BatchSize = ReadRequiredIntFromConfiguration(scenarioSection, "BatchSize", 1),
                    Writers = ReadWritersFromConfiguration(scenarioSection, "Writers"),
                    TotalChunks = ReadRequiredLongFromConfiguration(scenarioSection, "TotalChunks", 1)
                });
            }

            return scenarios;
        }

        private static perf_test_configuration ReadSingleScenarioFromEnvironment()
        {
            return new perf_test_configuration
            {
                Name = "env-default",
                BatchSize = ReadIntSetting(BatchSizeVar, 1_000, 1),
                Writers = ReadWritersSetting(WritersVar, 1),
                TotalChunks = ReadLongSetting(TotalChunksVar, 1_000, 1)
            };
        }

        private static int ReadWritersFromConfiguration(IConfigurationSection section, string key)
        {
            var raw = section[key];
            if (string.IsNullOrWhiteSpace(raw))
            {
                throw new ArgumentException(
                    $"{section.Path}:{key} must be configured.");
            }

            if (raw.Equals("unbounded", StringComparison.OrdinalIgnoreCase) ||
                raw.Equals("auto", StringComparison.OrdinalIgnoreCase))
            {
                return 0;
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{section.Path}:{key} must be an integer or 'unbounded', but was '{raw}'.");
            }

            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(
                    $"{section.Path}:{key}",
                    $"{section.Path}:{key} must be >= 0, but was {value}.");
            }

            return value;
        }

        private static int ReadWritersSetting(string variable, int fallback)
        {
            var raw = Environment.GetEnvironmentVariable(variable);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return fallback;
            }

            if (raw.Equals("unbounded", StringComparison.OrdinalIgnoreCase) ||
                raw.Equals("auto", StringComparison.OrdinalIgnoreCase))
            {
                return 0;
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{variable} must be an integer or 'unbounded', but was '{raw}'.");
            }

            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(
                    variable,
                    $"{variable} must be >= 0, but was {value}.");
            }

            return value;
        }

        private static int ReadRequiredIntFromConfiguration(
            IConfigurationSection section,
            string key,
            int minValue)
        {
            var raw = section[key];
            if (string.IsNullOrWhiteSpace(raw))
            {
                throw new ArgumentException(
                    $"{section.Path}:{key} must be configured.");
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{section.Path}:{key} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    $"{section.Path}:{key}",
                    $"{section.Path}:{key} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        private static long ReadRequiredLongFromConfiguration(
            IConfigurationSection section,
            string key,
            long minValue)
        {
            var raw = section[key];
            if (string.IsNullOrWhiteSpace(raw))
            {
                throw new ArgumentException(
                    $"{section.Path}:{key} must be configured.");
            }

            if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{section.Path}:{key} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    $"{section.Path}:{key}",
                    $"{section.Path}:{key} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        private static int ReadIntSetting(string variable, int fallback, int minValue)
        {
            var raw = Environment.GetEnvironmentVariable(variable);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return fallback;
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{variable} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    variable,
                    $"{variable} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        private static long ReadLongSetting(string variable, long fallback, long minValue)
        {
            var raw = Environment.GetEnvironmentVariable(variable);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return fallback;
            }

            if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{variable} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    variable,
                    $"{variable} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        private static int? ReadOptionalIntSetting(string variable, int minValue)
        {
            var raw = Environment.GetEnvironmentVariable(variable);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{variable} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    variable,
                    $"{variable} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        private static double? ReadDoubleSetting(string variable)
        {
            var raw = Environment.GetEnvironmentVariable(variable);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{variable} must be a floating-point value, but was '{raw}'.");
            }

            if (value < 1.0)
            {
                throw new ArgumentOutOfRangeException(
                    variable,
                    $"{variable} must be >= 1.0 (1.0 = no degradation), but was {value}.");
            }

            return value;
        }

        private static bool IsEnabled(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return value == "1" ||
                   value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                   value.Equals("yes", StringComparison.OrdinalIgnoreCase);
        }

        private static append_mode ReadAppendModeSetting(string variable)
        {
            var raw = Environment.GetEnvironmentVariable(variable);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return append_mode.baseline;
            }

            if (raw.Equals("baseline", StringComparison.OrdinalIgnoreCase) ||
                raw.Equals("default", StringComparison.OrdinalIgnoreCase))
            {
                return append_mode.baseline;
            }

            if (raw.Equals("parallel", StringComparison.OrdinalIgnoreCase) ||
                raw.Equals("parallel-extension", StringComparison.OrdinalIgnoreCase) ||
                raw.Equals("extension", StringComparison.OrdinalIgnoreCase))
            {
                return append_mode.parallel_extension;
            }

            throw new ArgumentException(
                $"{variable} must be one of: baseline, default, parallel, parallel-extension, extension. Value: '{raw}'.");
        }

        private enum append_mode
        {
            baseline,
            parallel_extension
        }

        private sealed class perf_test_configuration
        {
            public string Name { get; set; }
            public int BatchSize { get; set; }
            public int Writers { get; set; }
            public long TotalChunks { get; set; }
        }

        private sealed class perf_run_result
        {
            public string ScenarioName { get; set; }
            public int BatchSize { get; set; }
            public int WriterCount { get; set; }
            public long TotalChunks { get; set; }
            public int PartitionCount { get; set; }
            public long TotalBatches { get; set; }
            public double TotalElapsedSeconds { get; set; }
            public double OverallThroughputItemsPerSecond { get; set; }
            public double MedianSingleBatchMs { get; set; }
            public double RepresentativePayloadSizeBytes { get; set; }
            public double LastDegradation { get; set; }
            public double WorstDegradation { get; set; }
            public string ScenarioLogFile { get; set; }
        }

        private sealed class batch_execution_result
        {
            public batch_execution_result(
                int batchSize,
                double batchDurationMs)
            {
                BatchSize = batchSize;
                BatchDurationMs = batchDurationMs;
            }

            public int BatchSize { get; }
            public double BatchDurationMs { get; }
        }

        private sealed class complex_batch_document
        {
            public long DocumentId { get; set; }
            public string StreamKey { get; set; }
            public long StreamIndex { get; set; }
            public DateTime CreatedAtUtc { get; set; }
            public string DocumentTitle { get; set; }
            public string DocumentSummary { get; set; }
            public string DocumentDescription { get; set; }
            public string DomainNarrative { get; set; }
            public string LifecycleNarrative { get; set; }
            public string ComplianceNarrative { get; set; }
            public string SecurityNarrative { get; set; }
            public string OwnershipNarrative { get; set; }
            public string ProcessNarrative { get; set; }
            public string AuditNarrative { get; set; }
            public metadata_document Metadata { get; set; }
            public nested_document[] NestedDocuments { get; set; }
        }

        private sealed class metadata_document
        {
            public string Tenant { get; set; }
            public string Region { get; set; }
            public string Source { get; set; }
            public string Classification { get; set; }
            public string GovernanceModel { get; set; }
            public string RetentionPolicy { get; set; }
            public string ProcessingProfile { get; set; }
            public string SecurityProfile { get; set; }
            public string SupportModel { get; set; }
            public string ChangeLog { get; set; }
            public string Notes { get; set; }
            public string[] Flags { get; set; }
        }

        private sealed class nested_document
        {
            public string NestedId { get; set; }
            public int Order { get; set; }
            public bool Active { get; set; }
            public double Score { get; set; }
            public string NestedTitle { get; set; }
            public string NestedSummary { get; set; }
            public string NestedDescription { get; set; }
            public string BusinessContext { get; set; }
            public string OperationalContext { get; set; }
            public string QualityContext { get; set; }
            public string ComplianceContext { get; set; }
            public string SecurityContext { get; set; }
            public string OwnershipContext { get; set; }
            public string TraceContext { get; set; }
            public nested_metrics_document Metrics { get; set; }
            public child_document[] Children { get; set; }
        }

        private sealed class nested_metrics_document
        {
            public int Count { get; set; }
            public double Ratio { get; set; }
            public string[] Labels { get; set; }
            public string MetricOverview { get; set; }
            public string MetricWindowDefinition { get; set; }
            public string MetricComputationNotes { get; set; }
            public string MetricDataSource { get; set; }
            public string MetricConfidenceExplanation { get; set; }
            public string MetricNormalizationRule { get; set; }
            public string MetricAlertPolicy { get; set; }
            public string MetricSamplingPlan { get; set; }
            public string MetricAuditNote { get; set; }
            public string MetricTrace { get; set; }
        }

        private sealed class child_document
        {
            public string ChildId { get; set; }
            public string Kind { get; set; }
            public int Value { get; set; }
            public string ChildTitle { get; set; }
            public string ChildSummary { get; set; }
            public string ChildDescription { get; set; }
            public string ChildContext { get; set; }
            public string ChildLifecycle { get; set; }
            public string ChildCompliance { get; set; }
            public string ChildSecurity { get; set; }
            public string ChildOwnership { get; set; }
            public string ChildProcessing { get; set; }
            public string ChildAudit { get; set; }
        }

        private sealed class writer_partition_state
        {
            public writer_partition_state(int partitionStart, int partitionCount)
            {
                PartitionStart = partitionStart;
                PartitionCount = partitionCount;
            }

            public int PartitionStart { get; }
            public int PartitionCount { get; }
            public long NextOffset { get; set; }
        }
    }
}
