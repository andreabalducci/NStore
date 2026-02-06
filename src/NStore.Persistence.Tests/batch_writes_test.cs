using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;
using NStore.Tpl;
using Xunit;

#pragma warning disable S101 // Types should be named in camel case
#pragma warning disable IDE1006 // Naming Styles

// ReSharper disable InconsistentNaming
namespace NStore.Persistence.Tests
{
    public class batch_writes_test : BasePersistenceTest
    {
        [Fact]
        public async Task should_add_many()
        {
            //@@TODO enable test discovery 
            if (Batcher == null)
                return;

            var jobs = new[]
            {
                new WriteJob("a", 1, "first", null),
                new WriteJob("a", 2, "second", null),
            };

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None).ConfigureAwait(false);

            Assert.InRange(jobs[0].Position, 1, 2);
            Assert.InRange(jobs[1].Position, 1, 2);
        }

        [Fact]
        public async Task should_fail_on_adding_many()
        {
            if (Batcher == null)
                return;

            var jobs = new[]
            {
                new WriteJob("a", 1, "call me maybe", null),
                new WriteJob("a", 1, "call me maybe", null),
                new WriteJob("a", 2, "me too", "fail"),
                new WriteJob("a", 3, "me too", "fail"),
            };

            await Batcher.AppendBatchAsync(jobs, CancellationToken.None);

            Assert.NotEqual(0, jobs[0].Position);
            Assert.Equal(0, jobs[1].Position);
            Assert.NotEqual(0, jobs[2].Position);
            Assert.Equal(0, jobs[3].Position);

            var firstIndexResults = new[] { jobs[0].Result, jobs[1].Result };

            Assert.Contains(firstIndexResults, result => result == WriteJob.WriteResult.Committed);
            Assert.Contains(firstIndexResults, result => result == WriteJob.WriteResult.DuplicatedIndex);

            var secondIndexResults = new[] { jobs[2].Result, jobs[3].Result };
            Assert.Contains(secondIndexResults, result => result == WriteJob.WriteResult.Committed);
            Assert.Contains(secondIndexResults, result => result == WriteJob.WriteResult.DuplicatedOperation);

            var a1 = await Store.ReadSingleBackwardAsync("a", 1, CancellationToken.None);
            var a2 = await Store.ReadSingleBackwardAsync("a", 2, CancellationToken.None);

            Assert.NotNull(a1);
            Assert.NotNull(a2);

            Assert.Equal<object>("call me maybe", a1.Payload);
            Assert.Equal<object>("me too", a2.Payload);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        [Fact]
        public async Task async_write_jobs()
        {
            if (Batcher == null)
                return;

            // note: insert order is not guaranteed, failures can appen on odd rows
            var jobs = new[]
            {
                new AsyncWriteJob("a", 1, "first", null),
                new AsyncWriteJob("a", 1, "fail here", null),
                new AsyncWriteJob("a", 2, "second", "fail"),
                new AsyncWriteJob("a", 3, "fail here too", "fail"),
            };

            var forget = Batcher.AppendBatchAsync(jobs, CancellationToken.None);

            var allTasks = jobs.Select(x => x.Task).ToArray();
            var written = await Task.WhenAll(allTasks);

            Assert.Equal(4, written.Length);
            Assert.NotNull(written[0]);
            Assert.Null(written[1]);
            Assert.NotNull(written[2]);
            Assert.Null(written[3]);
        }

        [Fact]
        public async Task write_with_batcher()
        {
            if (Batcher == null)
                return;

            using var cts = new CancellationTokenSource(10_000);
            await using var batcher = new PersistenceBatchAppendDecorator(_persistence, _logger, 512, 10);
            //            batcher.Cancel(10_000);

            await batcher.AppendAsync("a", 1, "first", null, cts.Token);
            //            await Assert.ThrowsAsync<DuplicateStreamIndexException>(() => batcher.AppendAsync("a", 1, "fail here"));

            var lastPos = await Store.ReadLastPositionAsync(cancellationToken: cts.Token);

            Assert.Equal(1, lastPos);
        }

        [Fact]
        public async Task should_add_many_with_parallel_batch_extension()
        {
            if (Batcher == null)
                return;

            var jobs = Enumerable.Range(0, 200)
                .Select(i => new WriteJob(
                    partitionId: $"p{i % 5}",
                    index: i / 5 + 1,
                    payload: $"payload-{i}",
                    operationId: $"op-{i}"))
                .ToArray();

            var options = new ParallelBatchAppendOptions
            {
                BatchSize = 17,
                MaxWriters = 4,
            };

            await Batcher.AppendBatchAsync(jobs, options, CancellationToken.None).ConfigureAwait(false);

            Assert.All(jobs, job =>
            {
                Assert.Equal(WriteJob.WriteResult.Committed, job.Result);
                Assert.NotEqual(0, job.Position);
            });

            var lastPosition = await Store.ReadLastPositionAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(jobs.Length, lastPosition);
        }
    }
}
