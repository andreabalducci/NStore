namespace NStore.Core.Persistence;

public sealed class ParallelBatchAppendOptions
{
    public int BatchSize { get; set; } = 1000;

    public int MaxWriters { get; set; } = 4;
}
