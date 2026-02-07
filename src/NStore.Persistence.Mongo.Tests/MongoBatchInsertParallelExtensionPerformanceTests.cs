using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using Xunit;
using Xunit.Abstractions;

namespace NStore.Persistence.Mongo.Tests
{
    // ReSharper disable once InconsistentNaming
    public class mongodb_parallel_extension_batch_insert_performance_tests : MongoBatchInsertPerformanceTestBase
    {
        public mongodb_parallel_extension_batch_insert_performance_tests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        [Trait("Category", "Performance")]
        public async Task should_measure_parallel_extension_batch_insert_performance_degradation()
        {
            if (TrySkipWhenPerfDisabled())
            {
                return;
            }

            var parallelOptions = ReadParallelOptionsFromConfiguration();
            var partitionCount = ReadIntSetting(PartitionCountConfigKey, 100, 2);
            var warmupBatches = ReadIntSetting(WarmupBatchesConfigKey, 3, 0);
            var maxDegradation = ReadDoubleSetting(MaxDegradationConfigKey);
            var configuredProgressEveryBatches = ReadOptionalIntSetting(ProgressEveryBatchesConfigKey, 1);
            var scenarios = LoadScenarios(partitionCount);

            var suiteLogFile = ResolveSuiteLogFilePath(
                ReadStringSetting(SuiteLogFileConfigKey),
                "extension-method");
            var suiteDirectory = Path.GetDirectoryName(suiteLogFile);
            if (!string.IsNullOrWhiteSpace(suiteDirectory))
            {
                Directory.CreateDirectory(suiteDirectory);
            }

            var mongoTargetUrl = ResolveSanitizedMongoTargetUrl();
            WriteSuiteStart(mongoTargetUrl, suiteLogFile, scenarios.Count, partitionCount, warmupBatches);

            await using var suiteWriter = new StreamWriter(suiteLogFile, append: false);
            await WriteSuiteHeaderAsync(
                suiteWriter,
                mongoTargetUrl,
                "extension-method",
                scenarios.Count,
                partitionCount,
                warmupBatches,
                configuredProgressEveryBatches).ConfigureAwait(false);

            var persistence = Create(true);
            if (!(persistence is IEnhancedPersistence batcher))
            {
                throw new InvalidOperationException("Persistence does not expose AppendBatchAsync.");
            }

            var runResults = new List<PerfRunResult>(scenarios.Count);
            for (var scenarioIndex = 0; scenarioIndex < scenarios.Count; scenarioIndex++)
            {
                var scenario = scenarios[scenarioIndex];
                var scenarioLogFile = ResolveScenarioLogFilePath(suiteLogFile, scenario, scenarioIndex + 1);
                var result = await RunExtensionScenarioAsync(
                    batcher,
                    scenario,
                    partitionCount,
                    warmupBatches,
                    configuredProgressEveryBatches,
                    maxDegradation,
                    mongoTargetUrl,
                    scenarioLogFile,
                    parallelOptions).ConfigureAwait(false);

                runResults.Add(result);
                var suiteLine = BuildSuiteLine(result);

                Output.WriteLine(suiteLine);
                await suiteWriter.WriteLineAsync(suiteLine);
                await suiteWriter.FlushAsync();

                if (scenarioIndex + 1 < scenarios.Count)
                {
                    var cooldownMessage =
                        $"Mongo batch benchmark cooldown: waiting {InterScenarioDelaySeconds}s before next scenario to reduce server oplog pressure.";
                    Output.WriteLine(cooldownMessage);
                    await Task.Delay(TimeSpan.FromSeconds(InterScenarioDelaySeconds)).ConfigureAwait(false);
                }
            }

            var summaryLine = BuildSuiteSummaryLine(runResults);

            Output.WriteLine(summaryLine);
            await suiteWriter.WriteLineAsync(summaryLine);
            await suiteWriter.FlushAsync();
        }

        private async Task<PerfRunResult> RunExtensionScenarioAsync(
            IEnhancedPersistence batcher,
            PerfTestConfiguration scenario,
            int partitionCount,
            int warmupBatches,
            int? configuredProgressEveryBatches,
            double? maxDegradation,
            string mongoTargetUrl,
            string logFile,
            ParallelBatchAppendOptions parallelOptions)
        {
            var configuredWriterCount = ResolveEffectiveWriterCount(
                scenario.Writers,
                CalculateTotalBatches(scenario),
                partitionCount);

            var resolvedParallelOptions = new ParallelBatchAppendOptions
            {
                BatchSize = parallelOptions.BatchSize > 0 ? parallelOptions.BatchSize : scenario.BatchSize,
                MaxWriters = parallelOptions.MaxWriters > 0 ? parallelOptions.MaxWriters : configuredWriterCount
            };

            if (scenario.TotalChunks > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(scenario.TotalChunks),
                    $"Scenario '{scenario.Name}' requires {scenario.TotalChunks} jobs, which exceeds the maximum supported queue size ({int.MaxValue}).");
            }

            var totalBatches = (scenario.TotalChunks + resolvedParallelOptions.BatchSize - 1) /
                               resolvedParallelOptions.BatchSize;
            var effectiveWriterCount = resolvedParallelOptions.MaxWriters;
            var progressEveryBatches = ResolveProgressEveryBatches(configuredProgressEveryBatches, totalBatches);
            var representativePayloadSizeBytes = EstimatePayloadSizeBytes(
                CreateComplexPayload(1, "perf-stream-0000", 1));
            var appendModeLabel =
                $"extension-method(batch_size={resolvedParallelOptions.BatchSize},max_writers={resolvedParallelOptions.MaxWriters})";

            await using var writer = new StreamWriter(logFile, append: false);
            await WriteScenarioHeaderAsync(
                writer,
                mongoTargetUrl,
                appendModeLabel,
                scenario,
                effectiveWriterCount,
                partitionCount,
                totalBatches,
                warmupBatches,
                progressEveryBatches).ConfigureAwait(false);
            WriteScenarioStart(
                scenario,
                appendModeLabel,
                effectiveWriterCount,
                partitionCount,
                totalBatches,
                warmupBatches,
                progressEveryBatches,
                mongoTargetUrl,
                logFile);

            {
                var singleWriterState = new WriterPartitionState(0, partitionCount);
                long nextId = 0;
                long warmupInserted = 0;
                var warmupTotalChunks = (long)warmupBatches * scenario.BatchSize;
                if (warmupTotalChunks > int.MaxValue)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(warmupBatches),
                        $"Warmup for scenario '{scenario.Name}' requires {warmupTotalChunks} jobs, which exceeds the maximum supported queue size ({int.MaxValue}).");
                }

                if (warmupTotalChunks > 0)
                {
                    var warmupJobs = CreateJobs(nextId, (int)warmupTotalChunks, singleWriterState);
                    nextId += warmupTotalChunks;
                    warmupInserted += warmupTotalChunks;

                    await batcher.AppendBatchAsync(warmupJobs, resolvedParallelOptions, CancellationToken.None).ConfigureAwait(false);
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

                long completedBatches = 0;
                async Task ProcessBatchResultAsync(BatchExecutionResult result)
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
                        return;
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
                    var line = BuildScenarioProgressLine(
                        completedBatches,
                        insertedTotal,
                        scenario,
                        progressPct,
                        windowBatchCount,
                        windowAvgBatchMs,
                        windowP95BatchMs,
                        windowThroughput,
                        elapsedTotalSeconds,
                        degradation,
                        lastPosition);

                    Output.WriteLine(line);
                    await writer.WriteLineAsync(line);
                    await writer.FlushAsync();

                    windowDurationSamples.Clear();
                    windowSampleSeen = 0;
                    windowBatchCount = 0;
                    windowBatchDurationSumMs = 0;
                    windowInserted = 0;
                    windowStopwatch.Restart();
                }

                var jobs = CreateJobs(nextId, (int)scenario.TotalChunks, singleWriterState);
                var batchStopwatch = Stopwatch.StartNew();
                await batcher.AppendBatchAsync(jobs, resolvedParallelOptions, CancellationToken.None).ConfigureAwait(false);
                batchStopwatch.Stop();
                Assert.All(jobs, job => Assert.Equal(WriteJob.WriteResult.Committed, job.Result));

                await ProcessBatchResultAsync(
                        new BatchExecutionResult(
                            jobs.Length,
                            batchStopwatch.Elapsed.TotalMilliseconds))
                    .ConfigureAwait(false);

                totalStopwatch.Stop();
                var medianSingleBatchMs = overallDurationSamples.Count == 0
                    ? 0
                    : Percentile(overallDurationSamples, 0.50);

                var summaryLine = BuildScenarioSummaryLine(
                    scenario,
                    totalStopwatch.Elapsed.TotalSeconds,
                    medianSingleBatchMs,
                    representativePayloadSizeBytes,
                    lastDegradation,
                    worstDegradation);

                Output.WriteLine(summaryLine);
                Output.WriteLine($"Mongo batch scenario log: {logFile}");
                await writer.WriteLineAsync(summaryLine);
                await writer.FlushAsync();

                if (maxDegradation.HasValue)
                {
                    Assert.True(
                        worstDegradation <= maxDegradation.Value,
                        $"Scenario '{scenario.Name}' measured degradation {worstDegradation:F3}x exceeds limit {maxDegradation.Value:F3}x.");
                }

                return new PerfRunResult
                {
                    ScenarioName = scenario.Name,
                    BatchSize = scenario.BatchSize,
                    WriterCount = scenario.Writers,
                    EffectiveWriterCount = effectiveWriterCount,
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
        }
    }
}
