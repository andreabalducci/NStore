using System.Collections.Generic;

namespace NStore.Persistence.Mongo.Tests
{
    public sealed class MongoBatchInsertPerfSettings
    {
        public bool Enabled { get; set; }
        public string ConnectionString { get; set; } = string.Empty;
        public int PartitionCount { get; set; } = 100;
        public long DefaultTotalChunks { get; set; }
        public int ProgressEveryBatches { get; set; }
        public int WarmupBatches { get; set; } = 3;
        public double MaxDegradation { get; set; }
        public string ScenarioFilter { get; set; } = string.Empty;
        public string SuiteLogFile { get; set; } = string.Empty;
        public int ParallelBatchSize { get; set; }
        public int ParallelWriters { get; set; }
        public List<MongoBatchInsertPerfScenarioSettings> TestParameters { get; set; } =
            new List<MongoBatchInsertPerfScenarioSettings>();
    }

    public sealed class MongoBatchInsertPerfScenarioSettings
    {
        public int BatchSize { get; set; }
        public string Writers { get; set; } = string.Empty;
        public long? TotalChunks { get; set; }
    }
}
