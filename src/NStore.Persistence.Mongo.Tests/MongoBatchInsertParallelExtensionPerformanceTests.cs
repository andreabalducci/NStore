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
            var perfSettings = ReadPerformanceSettings();
            if (TrySkipWhenPerfDisabled(perfSettings))
            {
                return;
            }

            var parallelOptions = ReadParallelOptionsFromConfiguration(perfSettings);
            var partitionCount = perfSettings.PartitionCount;
            var warmupBatches = perfSettings.WarmupBatches;
            var configuredProgressEveryBatches = perfSettings.ProgressEveryBatches > 0 ? (int?)perfSettings.ProgressEveryBatches : null;
            var scenarios = LoadScenarios(perfSettings, partitionCount);

            var suiteLogFile = ResolveSuiteLogFilePath(
                perfSettings.SuiteLogFile,
                "extension-method");
            var suiteDirectory = Path.GetDirectoryName(suiteLogFile);
            if (!string.IsNullOrWhiteSpace(suiteDirectory))
            {
                Directory.CreateDirectory(suiteDirectory);
            }

            var mongoTargetUrl = ResolveSanitizedMongoTargetUrl();
            WriteSuiteStart(mongoTargetUrl, suiteLogFile, scenarios.Count, partitionCount, warmupBatches);

            await using var suiteWriter = await PerfCsvWriter.CreateSuiteAsync(
                    suiteLogFile,
                    mongoTargetUrl,
                    "extension-method",
                    scenarios.Count,
                    partitionCount,
                    warmupBatches,
                    configuredProgressEveryBatches)
                .ConfigureAwait(false);

            var runResults = new List<PerfRunResult>(scenarios.Count);
            for (var scenarioIndex = 0; scenarioIndex < scenarios.Count; scenarioIndex++)
            {
                var persistence = Create(true);
                if (!(persistence is IEnhancedPersistence batcher))
                {
                    throw new InvalidOperationException("Persistence does not expose AppendBatchAsync.");
                }

                var scenario = scenarios[scenarioIndex];
                var scenarioLogFile = ResolveScenarioLogFilePath(suiteLogFile, scenario, scenarioIndex + 1);
                var result = await RunExtensionScenarioAsync(
                    batcher,
                    scenario,
                    partitionCount,
                    warmupBatches,
                    configuredProgressEveryBatches,
                    mongoTargetUrl,
                    scenarioLogFile,
                    parallelOptions).ConfigureAwait(false);

                runResults.Add(result);
                var suiteLine = await suiteWriter.WriteSuiteResultAsync(result).ConfigureAwait(false);

                Output.WriteLine(suiteLine);

                if (scenarioIndex + 1 < scenarios.Count)
                {
                    var cooldownMessage =
                        $"Mongo batch benchmark cooldown: waiting {InterScenarioDelaySeconds}s before next scenario to reduce server oplog pressure.";
                    Output.WriteLine(cooldownMessage);
                    await Task.Delay(TimeSpan.FromSeconds(InterScenarioDelaySeconds)).ConfigureAwait(false);
                }
            }

            var summaryLine = await suiteWriter.WriteSuiteSummaryAsync(runResults).ConfigureAwait(false);

            Output.WriteLine(summaryLine);
        }

        private async Task<PerfRunResult> RunExtensionScenarioAsync(
            IEnhancedPersistence batcher,
            PerfTestConfiguration scenario,
            int partitionCount,
            int warmupBatches,
            int? configuredProgressEveryBatches,
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

            await using var writer = await PerfCsvWriter.CreateScenarioAsync(
                    logFile,
                    mongoTargetUrl,
                    appendModeLabel,
                    scenario,
                    effectiveWriterCount,
                    partitionCount,
                    totalBatches,
                    warmupBatches,
                    progressEveryBatches)
                .ConfigureAwait(false);
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
                    var line = await writer.WriteScenarioProgressAsync(
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
                            lastPosition)
                        .ConfigureAwait(false);

                    Output.WriteLine(line);

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

                var summaryLine = await writer.WriteScenarioSummaryAsync(
                        scenario,
                        totalStopwatch.Elapsed.TotalSeconds,
                        medianSingleBatchMs,
                        representativePayloadSizeBytes,
                        lastDegradation,
                        worstDegradation)
                    .ConfigureAwait(false);

                Output.WriteLine(summaryLine);
                Output.WriteLine($"Mongo batch scenario log: {logFile}");

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
