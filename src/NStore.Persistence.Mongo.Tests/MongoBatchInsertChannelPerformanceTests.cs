using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using Xunit;
using Xunit.Abstractions;

namespace NStore.Persistence.Mongo.Tests
{
    // ReSharper disable once InconsistentNaming
    public class mongodb_batch_insert_performance_tests : MongoBatchInsertPerformanceTestBase
    {
        public mongodb_batch_insert_performance_tests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        [Trait("Category", "Performance")]
        public async Task should_measure_batch_insert_performance_degradation()
        {
            if (TrySkipWhenPerfDisabled())
            {
                return;
            }

            var partitionCount = ReadIntSetting(PartitionCountConfigKey, 100, 2);
            var warmupBatches = ReadIntSetting(WarmupBatchesConfigKey, 3, 0);
            var maxDegradation = ReadDoubleSetting(MaxDegradationConfigKey);
            var configuredProgressEveryBatches = ReadOptionalIntSetting(ProgressEveryBatchesConfigKey, 1);
            var scenarios = LoadScenarios(partitionCount);

            var suiteLogFile = ResolveSuiteLogFilePath(
                ReadStringSetting(SuiteLogFileConfigKey),
                "channel-workers");
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
                    "channel-workers",
                    scenarios.Count,
                    partitionCount,
                    warmupBatches,
                    configuredProgressEveryBatches)
                .ConfigureAwait(false);

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
                var result = await RunChannelScenarioAsync(
                    batcher,
                    scenario,
                    partitionCount,
                    warmupBatches,
                    configuredProgressEveryBatches,
                    maxDegradation,
                    mongoTargetUrl,
                    scenarioLogFile).ConfigureAwait(false);

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

        private async Task<PerfRunResult> RunChannelScenarioAsync(
            IEnhancedPersistence batcher,
            PerfTestConfiguration scenario,
            int partitionCount,
            int warmupBatches,
            int? configuredProgressEveryBatches,
            double? maxDegradation,
            string mongoTargetUrl,
            string logFile)
        {
            var totalBatches = CalculateTotalBatches(scenario);

            var effectiveWriterCount = ResolveEffectiveWriterCount(
                scenario.Writers,
                totalBatches,
                partitionCount);

            var progressEveryBatches = ResolveProgressEveryBatches(configuredProgressEveryBatches, totalBatches);
            var representativePayloadSizeBytes = EstimatePayloadSizeBytes(
                CreateComplexPayload(1, "perf-stream-0000", 1));
            var appendModeLabel = $"channel-workers(effective_writers={effectiveWriterCount})";

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

            CancellationTokenSource workerCancellation = null;
            Task workersCompletionTask = Task.CompletedTask;
            try
            {
                var writerStates = CreateWriterStates(effectiveWriterCount, partitionCount);

                long nextId = 0;
                long warmupInserted = 0;
                for (var i = 0; i < warmupBatches; i++)
                {
                    var writerState = writerStates[i % writerStates.Length];
                    var warmupJobs = CreateJobs(nextId, scenario.BatchSize, writerState);
                    nextId += scenario.BatchSize;
                    warmupInserted += scenario.BatchSize;

                    await batcher.AppendBatchAsync(warmupJobs, CancellationToken.None).ConfigureAwait(false);
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

                var batchResults = Channel.CreateUnbounded<BatchExecutionResult>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false
                });

                var nextPayloadId = nextId;
                workerCancellation = new CancellationTokenSource();
                var workerCancellationToken = workerCancellation.Token;

                long nextBatchNumber = 0;
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
                                await batcher.AppendBatchAsync(jobs, workerCancellationToken).ConfigureAwait(false);
                                batchStopwatch.Stop();
                                Assert.All(jobs, job => Assert.Equal(WriteJob.WriteResult.Committed, job.Result));

                                await batchResults.Writer.WriteAsync(
                                        new BatchExecutionResult(
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

                await foreach (var result in batchResults.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    await ProcessBatchResultAsync(result).ConfigureAwait(false);
                }

                await workersCompletionTask.ConfigureAwait(false);

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
            finally
            {
                if (workerCancellation != null)
                {
                    workerCancellation.Cancel();
                }

                // Give workers a bounded window to finish after cancellation;
                // avoids hanging the test host if a MongoDB operation ignores the token.
                var completed = await Task.WhenAny(
                    workersCompletionTask,
                    Task.Delay(TimeSpan.FromSeconds(WorkerShutdownTimeoutSeconds))
                ).ConfigureAwait(false);

                if (completed != workersCompletionTask)
                {
                    Output.WriteLine(
                        $"Warning: worker tasks did not complete within {WorkerShutdownTimeoutSeconds}s after cancellation.");
                }

                workerCancellation?.Dispose();
            }
        }
    }
}
