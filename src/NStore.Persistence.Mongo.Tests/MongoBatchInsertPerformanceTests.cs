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
    public abstract class MongoBatchInsertPerformanceTestBase : BasePersistenceTest
    {
        private const string PerformanceConfigPath = "NStore:Mongo:Performance";
        protected const string PerfEnabledConfigKey = PerformanceConfigPath + ":Enabled";
        protected const string PerformanceProfilesConfigPath = PerformanceConfigPath + ":TestParameters";
        protected const string PartitionCountConfigKey = PerformanceConfigPath + ":PartitionCount";
        protected const string ProgressEveryBatchesConfigKey = PerformanceConfigPath + ":ProgressEveryBatches";
        protected const string WarmupBatchesConfigKey = PerformanceConfigPath + ":WarmupBatches";
        protected const string MaxDegradationConfigKey = PerformanceConfigPath + ":MaxDegradation";
        protected const string ScenarioFilterConfigKey = PerformanceConfigPath + ":ScenarioFilter";
        protected const string SuiteLogFileConfigKey = PerformanceConfigPath + ":SuiteLogFile";
        private const string ParallelBatchSizeConfigKey = PerformanceConfigPath + ":ParallelBatchSize";
        private const string ParallelWritersConfigKey = PerformanceConfigPath + ":ParallelWriters";
        protected const int InterScenarioDelaySeconds = 5;
        protected const int WorkerShutdownTimeoutSeconds = 30;
        private static readonly string[] PerfMongoConnectionConfigKeys =
        {
            "NStore:Mongo:Performance:ConnectionString"
        };

        private static readonly string[] MongoConnectionConfigKeys =
        {
            "NStore:Mongo:ConnectionString"
        };

        protected const int DurationSampleLimit = 4_096;
        private const string LongTextTemplate =
            "This benchmark payload is intentionally verbose to stress serialization, indexing, and storage paths with deterministic long-form content that can be reproduced across runs and compared over time for regression analysis.";

        private readonly ITestOutputHelper _output;

        protected MongoBatchInsertPerformanceTestBase(ITestOutputHelper output) : base(false)
        {
            _output = output;
        }
        protected ITestOutputHelper Output => _output;

        protected bool TrySkipWhenPerfDisabled()
        {
            if (ReadBoolSetting(PerfEnabledConfigKey, fallback: false))
            {
                return false;
            }

            _output.WriteLine(
                $"Skipping: set {PerfEnabledConfigKey}=true in appsettings/appsettings.Local.json/user-secrets to run this test.");
            return true;
        }

        protected static List<PerfTestConfiguration> LoadScenarios(int partitionCount)
        {
            var scenarios = ReadConfiguredScenarios();
            if (scenarios.Count == 0)
            {
                throw new ArgumentException(
                    $"{PerformanceProfilesConfigPath} must include at least one scenario.");
            }

            scenarios = scenarios
                .OrderBy(x => x.Writers <= 0 ? int.MaxValue : x.Writers)
                .ThenBy(x => x.BatchSize)
                .ThenBy(x => x.Name)
                .ToList();

            var scenarioFilter = ReadStringSetting(ScenarioFilterConfigKey);
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
                        $"{ScenarioFilterConfigKey} did not match any configured scenario name in {PerformanceProfilesConfigPath}.");
                }
            }

            foreach (var scenario in scenarios)
            {
                if (scenario.Writers > 0 && scenario.Writers > partitionCount)
                {
                    throw new ArgumentOutOfRangeException(
                        PerformanceProfilesConfigPath,
                        $"Scenario '{scenario.Name}' has writers={scenario.Writers}, but {PartitionCountConfigKey} is {partitionCount}. Writers must be <= partitions.");
                }
            }

            return scenarios;
        }

        protected static long CalculateTotalBatches(PerfTestConfiguration scenario)
        {
            var totalBatches = scenario.TotalChunks / scenario.BatchSize;
            if (scenario.TotalChunks % scenario.BatchSize != 0)
            {
                totalBatches++;
            }

            return totalBatches;
        }

        protected static long ResolveProgressEveryBatches(int? configuredProgressEveryBatches, long totalBatches)
        {
            var progressEveryBatches = configuredProgressEveryBatches.HasValue
                ? configuredProgressEveryBatches.Value
                : Math.Max(1L, totalBatches / 20);
            return Math.Min(progressEveryBatches, totalBatches);
        }

        protected void WriteSuiteStart(string mongoTargetUrl, string suiteLogFile, int scenarioCount, int partitionCount, int warmupBatches)
        {
            Output.WriteLine($"Mongo batch benchmark suite: scenarios={scenarioCount}, partitions={partitionCount}, warmup={warmupBatches}.");
            Output.WriteLine($"Mongo batch benchmark target: {mongoTargetUrl}");
            Output.WriteLine($"Mongo batch benchmark suite log: {suiteLogFile}");
        }

        protected void WriteScenarioStart(
            PerfTestConfiguration scenario,
            string appendModeLabel,
            int effectiveWriterCount,
            int partitionCount,
            long totalBatches,
            int warmupBatches,
            long progressEveryBatches,
            string mongoTargetUrl,
            string logFile)
        {
            Output.WriteLine(
                $"Mongo batch scenario: name={scenario.Name}, appendMode={appendModeLabel}, totalChunks={scenario.TotalChunks}, batchSize={scenario.BatchSize}, writers={FormatWriters(scenario.Writers)}, effectiveWriters={effectiveWriterCount}, partitions={partitionCount}, totalBatches={totalBatches}, warmup={warmupBatches}, progressEveryBatches={progressEveryBatches}.");
            Output.WriteLine($"Mongo batch scenario target: {mongoTargetUrl}");
            Output.WriteLine($"Mongo batch scenario log: {logFile}");
        }

        protected static ParallelBatchAppendOptions ReadParallelOptionsFromConfiguration()
        {
            var parallelBatchSize = ReadOptionalIntSetting(ParallelBatchSizeConfigKey, 1);
            var parallelWriters = ReadOptionalIntSetting(ParallelWritersConfigKey, 1);
            return new ParallelBatchAppendOptions
            {
                BatchSize = parallelBatchSize ?? 0,
                MaxWriters = parallelWriters ?? 0
            };
        }

        protected static WriteJob[] CreateJobs(
            long startId,
            int batchSize,
            WriterPartitionState writerState)
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

        protected static WriterPartitionState[] CreateWriterStates(int writerCount, int partitionCount)
        {
            var writerStates = new WriterPartitionState[writerCount];
            var basePartitionsPerWriter = partitionCount / writerCount;
            var remainder = partitionCount % writerCount;
            var nextPartitionStart = 0;

            for (var writer = 0; writer < writerCount; writer++)
            {
                var assignedPartitions = basePartitionsPerWriter + (writer < remainder ? 1 : 0);
                writerStates[writer] = new WriterPartitionState(nextPartitionStart, assignedPartitions);
                nextPartitionStart += assignedPartitions;
            }

            return writerStates;
        }

        protected static int EstimatePayloadSizeBytes(ComplexBatchDocument payload)
        {
            return payload.ToBson().Length;
        }

        protected static ComplexBatchDocument CreateComplexPayload(
            long currentId,
            string partitionId,
            long streamIndex)
        {
            var now = DateTime.UtcNow;
            var nestedDocuments = Enumerable.Range(1, 5)
                .Select(i => new NestedDocument
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
                    Metrics = new NestedMetricsDocument
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
                        new ChildDocument
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
                        new ChildDocument
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

            return new ComplexBatchDocument
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
                Metadata = new MetadataDocument
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

        protected static double Percentile(IReadOnlyList<double> values, double percentile)
        {
            var ordered = values.OrderBy(x => x).ToArray();
            var index = (int)Math.Ceiling(percentile * ordered.Length) - 1;
            index = Math.Max(0, Math.Min(index, ordered.Length - 1));
            return ordered[index];
        }

        protected static void AddDurationSample(
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

        protected static bool IsCancellationException(Exception exception)
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

        protected static string ResolveSuiteLogFilePath(string configuredPath, string testMethodName)
        {
            if (!string.IsNullOrWhiteSpace(configuredPath))
            {
                return Path.GetFullPath(configuredPath);
            }

            var runFolder = $"{testMethodName}-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
            return Path.GetFullPath(Path.Combine(
                Directory.GetCurrentDirectory(), "TestResults", runFolder, "suite.csv"));
        }

        protected static string ResolveScenarioLogFilePath(
            string suiteLogFile,
            PerfTestConfiguration scenario,
            int scenarioIndex)
        {
            var directory = Path.GetDirectoryName(suiteLogFile);
            if (string.IsNullOrWhiteSpace(directory))
            {
                directory = Directory.GetCurrentDirectory();
            }

            var scenarioName = SanitizeFileName(scenario.Name);
            var fileName = string.Format(
                CultureInfo.InvariantCulture,
                "s{0:D2}-{1}-b{2}-w{3}-c{4}.csv",
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

        protected static string ResolveSanitizedMongoTargetUrl()
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
            var config = GetTestConfiguration();
            if (ReadBoolSetting(config, PerfEnabledConfigKey, fallback: false))
            {
                var perfMongo = ReadFirstConfiguredValue(config, PerfMongoConnectionConfigKeys);
                if (!string.IsNullOrWhiteSpace(perfMongo))
                {
                    return perfMongo;
                }
            }

            var mongo = ReadFirstConfiguredValue(config, MongoConnectionConfigKeys);
            if (!string.IsNullOrWhiteSpace(mongo))
            {
                return mongo;
            }

            throw new TestMisconfiguredException(
                $"Mongo connection string not set. Configure appsettings/user-secrets keys: {string.Join(", ", PerfMongoConnectionConfigKeys)} (when {PerfEnabledConfigKey}=true) or {string.Join(", ", MongoConnectionConfigKeys)}.");
        }

        private static string ReadFirstConfiguredValue(IConfiguration config, string[] keys)
        {
            return keys
                .Select(t => config[t])
                .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));
        }

        protected static int ResolveEffectiveWriterCount(
            int configuredWriters,
            long totalBatches,
            int partitionCount)
        {
            return configuredWriters > 0 
                ? configuredWriters 
                : Math.Max(1, (int)Math.Min(totalBatches, partitionCount));
        }

        protected static string FormatWriters(int configuredWriters)
        {
            return configuredWriters <= 0
                ? "unbounded"
                : configuredWriters.ToString(CultureInfo.InvariantCulture);
        }

        protected static List<PerfTestConfiguration> ReadConfiguredScenarios()
        {
            var config = GetTestConfiguration();
            var scenarioSections = config.GetSection(PerformanceProfilesConfigPath).GetChildren().ToArray();
            if (scenarioSections.Length == 0)
            {
                return new List<PerfTestConfiguration>();
            }

            var scenarios = new List<PerfTestConfiguration>(scenarioSections.Length);
            for (var i = 0; i < scenarioSections.Length; i++)
            {
                var scenarioSection = scenarioSections[i];
                var rawName = scenarioSection["Name"];
                var name = string.IsNullOrWhiteSpace(rawName)
                    ? $"scenario-{i + 1}"
                    : rawName.Trim();

                scenarios.Add(new PerfTestConfiguration
                {
                    Name = name,
                    BatchSize = ReadRequiredIntFromConfiguration(scenarioSection, "BatchSize", 1),
                    Writers = ReadWritersFromConfiguration(scenarioSection, "Writers"),
                    TotalChunks = ReadRequiredLongFromConfiguration(scenarioSection, "TotalChunks", 1)
                });
            }

            return scenarios;
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

        protected static int ReadIntSetting(string configKey, int fallback, int minValue)
        {
            var raw = ReadStringSetting(configKey);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return fallback;
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{configKey} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    configKey,
                    $"{configKey} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        protected static int? ReadOptionalIntSetting(string configKey, int minValue)
        {
            var raw = ReadStringSetting(configKey);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{configKey} must be an integer value, but was '{raw}'.");
            }

            if (value < minValue)
            {
                throw new ArgumentOutOfRangeException(
                    configKey,
                    $"{configKey} must be >= {minValue}, but was {value}.");
            }

            return value;
        }

        protected static double? ReadDoubleSetting(string configKey)
        {
            var raw = ReadStringSetting(configKey);
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            {
                throw new ArgumentException(
                    $"{configKey} must be a floating-point value, but was '{raw}'.");
            }

            if (value < 1.0)
            {
                throw new ArgumentOutOfRangeException(
                    configKey,
                    $"{configKey} must be >= 1.0 (1.0 = no degradation), but was {value}.");
            }

            return value;
        }

        protected static bool ReadBoolSetting(string configKey, bool fallback)
        {
            return ReadBoolSetting(GetTestConfiguration(), configKey, fallback);
        }

        protected static string ReadStringSetting(string configKey)
        {
            var raw = GetTestConfiguration()[configKey];
            return string.IsNullOrWhiteSpace(raw)
                ? null
                : raw.Trim();
        }

        protected static bool IsEnabled(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return value == "1" ||
                   value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                   value.Equals("yes", StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsDisabled(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            return value == "0" ||
                   value.Equals("false", StringComparison.OrdinalIgnoreCase) ||
                   value.Equals("no", StringComparison.OrdinalIgnoreCase);
        }

        private static bool ReadBoolSetting(IConfiguration config, string configKey, bool fallback)
        {
            var raw = config[configKey];
            if (string.IsNullOrWhiteSpace(raw))
            {
                return fallback;
            }

            if (IsEnabled(raw))
            {
                return true;
            }

            if (IsDisabled(raw))
            {
                return false;
            }

            throw new ArgumentException(
                $"{configKey} must be a boolean value (supported: true/false, yes/no, 1/0), but was '{raw}'.");
        }

        protected sealed class PerfCsvWriter : IAsyncDisposable
        {
            private readonly StreamWriter _writer;

            private PerfCsvWriter(StreamWriter writer)
            {
                _writer = writer;
            }

            public static async Task<PerfCsvWriter> CreateSuiteAsync(
                string logFile,
                string mongoTargetUrl,
                string testMethodName,
                int scenarioCount,
                int partitionCount,
                int warmupBatches,
                int? configuredProgressEveryBatches)
            {
                var writer = new PerfCsvWriter(new StreamWriter(logFile, append: false));
                await writer.WriteLineCoreAsync($"# mongodb_url={mongoTargetUrl}").ConfigureAwait(false);
                await writer.WriteLineCoreAsync($"# test_method={testMethodName}").ConfigureAwait(false);
                await writer.WriteLineCoreAsync($"# started_utc={DateTimeOffset.UtcNow:O}").ConfigureAwait(false);
                await writer.WriteLineCoreAsync(
                    $"# scenarios={scenarioCount},partitions={partitionCount},warmup_batches={warmupBatches},progress_every_batches={(configuredProgressEveryBatches.HasValue ? configuredProgressEveryBatches.Value.ToString(CultureInfo.InvariantCulture) : "auto")},inter_scenario_delay_s={InterScenarioDelaySeconds}")
                    .ConfigureAwait(false);
                await writer.WriteLineCoreAsync(
                    "scenario_name,batch_size,writers,effective_writers,total_chunks,partitions,total_batches,total_elapsed_s,overall_throughput_items_per_sec,median_single_batch_ms,representative_payload_size_bytes,last_degradation_x,worst_degradation_x,scenario_log_file")
                    .ConfigureAwait(false);
                return writer;
            }

            public static async Task<PerfCsvWriter> CreateScenarioAsync(
                string logFile,
                string mongoTargetUrl,
                string appendModeLabel,
                PerfTestConfiguration scenario,
                int effectiveWriterCount,
                int partitionCount,
                long totalBatches,
                int warmupBatches,
                long progressEveryBatches)
            {
                var writer = new PerfCsvWriter(new StreamWriter(logFile, append: false));
                await writer.WriteLineCoreAsync($"# mongodb_url={mongoTargetUrl}").ConfigureAwait(false);
                await writer.WriteLineCoreAsync($"# test_method={appendModeLabel}").ConfigureAwait(false);
                await writer.WriteLineCoreAsync($"# started_utc={DateTimeOffset.UtcNow:O}").ConfigureAwait(false);
                await writer.WriteLineCoreAsync(
                        $"# scenario={scenario.Name},append_mode={appendModeLabel},total_chunks={scenario.TotalChunks},batch_size={scenario.BatchSize},writers={FormatWriters(scenario.Writers)},effective_writers={effectiveWriterCount},partitions={partitionCount},total_batches={totalBatches},warmup_batches={warmupBatches},progress_every_batches={progressEveryBatches}")
                    .ConfigureAwait(false);
                await writer.WriteLineCoreAsync(
                        "timestamp_utc,batch,inserted_total,total_chunks,progress_pct,window_batches,window_avg_batch_ms,window_p95_batch_ms,window_throughput_items_per_sec,total_elapsed_s,degradation_x,last_position")
                    .ConfigureAwait(false);
                return writer;
            }

            public async Task<string> WriteSuiteResultAsync(PerfRunResult result)
            {
                var line = BuildSuiteLine(result);
                await WriteLineCoreAsync(line).ConfigureAwait(false);
                return line;
            }

            public async Task<string> WriteSuiteSummaryAsync(IReadOnlyList<PerfRunResult> runResults)
            {
                var line = BuildSuiteSummaryLine(runResults);
                await WriteLineCoreAsync(line).ConfigureAwait(false);
                return line;
            }

            public async Task<string> WriteScenarioProgressAsync(
                long completedBatches,
                long insertedTotal,
                PerfTestConfiguration scenario,
                double progressPct,
                long windowBatchCount,
                double windowAvgBatchMs,
                double windowP95BatchMs,
                double windowThroughput,
                double elapsedTotalSeconds,
                double degradation,
                long lastPosition)
            {
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
                await WriteLineCoreAsync(line).ConfigureAwait(false);
                return line;
            }

            public async Task<string> WriteScenarioSummaryAsync(
                PerfTestConfiguration scenario,
                double elapsedSeconds,
                double medianSingleBatchMs,
                double representativePayloadSizeBytes,
                double lastDegradation,
                double worstDegradation)
            {
                var line = BuildScenarioSummaryLine(
                    scenario,
                    elapsedSeconds,
                    medianSingleBatchMs,
                    representativePayloadSizeBytes,
                    lastDegradation,
                    worstDegradation);
                await WriteLineCoreAsync(line).ConfigureAwait(false);
                return line;
            }

            public ValueTask DisposeAsync()
            {
                return _writer.DisposeAsync();
            }

            private async Task WriteLineCoreAsync(string line)
            {
                await _writer.WriteLineAsync(line).ConfigureAwait(false);
                await _writer.FlushAsync().ConfigureAwait(false);
            }

            private static string BuildSuiteLine(PerfRunResult result)
            {
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "{0},{1},{2},{3},{4},{5},{6},{7:F2},{8:F0},{9:F2},{10:F0},{11:F3},{12:F3},{13}",
                    CsvEscape(result.ScenarioName),
                    result.BatchSize,
                    result.WriterCount,
                    result.EffectiveWriterCount,
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
            }

            private static string BuildSuiteSummaryLine(IReadOnlyList<PerfRunResult> runResults)
            {
                var totalSuiteElapsedSeconds = runResults.Sum(x => x.TotalElapsedSeconds);
                var bestThroughput = runResults.Count == 0 ? 0 : runResults.Max(x => x.OverallThroughputItemsPerSecond);
                var slowestMedianBatchMs = runResults.Count == 0 ? 0 : runResults.Max(x => x.MedianSingleBatchMs);
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "# summary,scenario_count={0},suite_total_elapsed_s={1:F2},best_overall_throughput_items_per_sec={2:F0},slowest_median_single_batch_ms={3:F2}",
                    runResults.Count,
                    totalSuiteElapsedSeconds,
                    bestThroughput,
                    slowestMedianBatchMs);
            }

            private static string BuildScenarioProgressLine(
                long completedBatches,
                long insertedTotal,
                PerfTestConfiguration scenario,
                double progressPct,
                long windowBatchCount,
                double windowAvgBatchMs,
                double windowP95BatchMs,
                double windowThroughput,
                double elapsedTotalSeconds,
                double degradation,
                long lastPosition)
            {
                return string.Format(
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
            }

            private static string BuildScenarioSummaryLine(
                PerfTestConfiguration scenario,
                double elapsedSeconds,
                double medianSingleBatchMs,
                double representativePayloadSizeBytes,
                double lastDegradation,
                double worstDegradation)
            {
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "# summary,total_chunks={0},total_elapsed_s={1:F2},median_single_batch_ms={2:F2},representative_payload_size_bytes={3:F0},last_degradation_x={4:F3},worst_degradation_x={5:F3}",
                    scenario.TotalChunks,
                    elapsedSeconds,
                    medianSingleBatchMs,
                    representativePayloadSizeBytes,
                    lastDegradation,
                    worstDegradation);
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
        }

        protected sealed class PerfTestConfiguration
        {
            public string Name { get; set; }
            public int BatchSize { get; set; }
            public int Writers { get; set; }
            public long TotalChunks { get; set; }
        }

        protected sealed class PerfRunResult
        {
            public string ScenarioName { get; set; }
            public int BatchSize { get; set; }
            public int WriterCount { get; set; }
            public int EffectiveWriterCount { get; set; }
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

        protected sealed class BatchExecutionResult
        {
            public BatchExecutionResult(
                int batchSize,
                double batchDurationMs)
            {
                BatchSize = batchSize;
                BatchDurationMs = batchDurationMs;
            }

            public int BatchSize { get; }
            public double BatchDurationMs { get; }
        }

        protected sealed class ComplexBatchDocument
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
            public MetadataDocument Metadata { get; set; }
            public NestedDocument[] NestedDocuments { get; set; }
        }

        protected sealed class MetadataDocument
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

        protected sealed class NestedDocument
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
            public NestedMetricsDocument Metrics { get; set; }
            public ChildDocument[] Children { get; set; }
        }

        protected sealed class NestedMetricsDocument
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

        protected sealed class ChildDocument
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

        protected sealed class WriterPartitionState
        {
            public WriterPartitionState(int partitionStart, int partitionCount)
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
