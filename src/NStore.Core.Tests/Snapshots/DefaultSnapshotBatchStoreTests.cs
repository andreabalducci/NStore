using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NStore.Core.Logging;
using NStore.Core.Snapshots;
using Xunit;

namespace NStore.Core.Tests.Snapshots
{
    public class DefaultSnapshotBatchStoreTests
    {
        /// <summary>
        /// A fake in-memory implementation of ISnapshotStore for testing purposes.
        /// </summary>
        private class FakeSnapshotStore : ISnapshotStore
        {
            private readonly Dictionary<string, SnapshotInfo> _snapshots = new Dictionary<string, SnapshotInfo>();

            private int _addAsyncCallCount;
            public int AddAsyncCallCount => _addAsyncCallCount;

            public void AddSnapshot(string partitionId, SnapshotInfo snapshot)
            {
                _snapshots[partitionId] = snapshot;
            }

            public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
            {
                _snapshots.TryGetValue(snapshotPartitionId, out var snapshot);
                return Task.FromResult(snapshot);
            }

            public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _addAsyncCallCount);
                _snapshots[snapshotPartitionId] = snapshot;
                return Task.CompletedTask;
            }

            public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private class ThrowingSnapshotStore : ISnapshotStore
        {
            public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
            {
                throw new InvalidOperationException("Simulated read failure");
            }

            public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
            {
                throw new InvalidOperationException("Simulated write failure");
            }

            public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        private class BatchCapableSnapshotStore : ISnapshotStore, ISnapshotStoreBatchWriter
        {
            private readonly Dictionary<string, SnapshotInfo> _snapshots = new Dictionary<string, SnapshotInfo>();

            private int _addAsyncCallCount;
            private int _addManyAsyncCallCount;

            public int AddAsyncCallCount => _addAsyncCallCount;
            public int AddManyAsyncCallCount => _addManyAsyncCallCount;

            public bool ThrowOnAddMany { get; set; }
            public bool CancelOnAddMany { get; set; }

            public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
            {
                _snapshots.TryGetValue(snapshotPartitionId, out var snapshot);
                return Task.FromResult(snapshot);
            }

            public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _addAsyncCallCount);
                _snapshots[snapshotPartitionId] = snapshot;
                return Task.CompletedTask;
            }

            public Task AddManyAsync(
                IReadOnlyDictionary<string, SnapshotInfo> snapshots,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _addManyAsyncCallCount);

                if (CancelOnAddMany)
                    throw new OperationCanceledException("Simulated batch cancellation");

                if (ThrowOnAddMany)
                    throw new InvalidOperationException("Simulated batch write failure");

                foreach (var kvp in snapshots)
                {
                    _snapshots[kvp.Key] = kvp.Value;
                }

                return Task.CompletedTask;
            }

            public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public void constructor_should_throw_when_snapshot_store_is_null()
        {
            // Act & Assert
            var exception = Assert.Throws<ArgumentNullException>(() => new DefaultSnapshotBatchStore(null));
            Assert.Equal("snapshotStore", exception.ParamName);
        }

        [Fact]
        public async Task get_many_async_should_throw_when_partition_ids_is_null()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                async () => await batchStore.GetManyAsync(null, CancellationToken.None)
            );
        }

        [Fact]
        public async Task get_many_async_should_return_empty_dictionary_when_no_partition_ids_provided()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            // Act
            var result = await batchStore.GetManyAsync(new string[0], CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public async Task get_many_async_should_return_snapshots_for_existing_partitions()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot2 = new SnapshotInfo("Order-2", 10, "payload2", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");

            store.AddSnapshot("Order-1", snapshot1);
            store.AddSnapshot("Order-2", snapshot2);
            store.AddSnapshot("Order-3", snapshot3);

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            var partitionIds = new[] { "Order-1", "Order-2", "Order-3" };

            // Act
            var result = await batchStore.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(3, result.Count);
            Assert.Same(snapshot1, result["Order-1"]);
            Assert.Same(snapshot2, result["Order-2"]);
            Assert.Same(snapshot3, result["Order-3"]);
        }

        [Fact]
        public async Task get_many_async_should_exclude_partitions_without_snapshots()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");

            store.AddSnapshot("Order-1", snapshot1);
            // Order-2 has no snapshot
            store.AddSnapshot("Order-3", snapshot3);

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            var partitionIds = new[] { "Order-1", "Order-2", "Order-3" };

            // Act
            var result = await batchStore.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.Count);
            Assert.False(result.ContainsKey("Order-2")); // Should not be in result
            Assert.True(result.TryGetValue("Order-1", out var order1Snapshot));
            Assert.Same(snapshot1, order1Snapshot);
            Assert.True(result.TryGetValue("Order-3", out var order3Snapshot));
            Assert.Same(snapshot3, order3Snapshot);
        }

        [Fact]
        public async Task get_many_async_should_deduplicate_partition_ids()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            store.AddSnapshot("Order-1", snapshot);

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            // Pass duplicate partition IDs
            var partitionIds = new[] { "Order-1", "Order-1", "Order-1" };

            // Act
            var result = await batchStore.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Single(result);
            Assert.Same(snapshot, result["Order-1"]);
        }

        [Fact]
        public async Task get_many_async_should_handle_mixed_existing_and_missing_partitions()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot5 = new SnapshotInfo("Order-5", 25, "payload5", "v1");

            store.AddSnapshot("Order-1", snapshot1);
            // Order-2, Order-3, Order-4 have no snapshots
            store.AddSnapshot("Order-5", snapshot5);

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            var partitionIds = new[] { "Order-1", "Order-2", "Order-3", "Order-4", "Order-5" };

            // Act
            var result = await batchStore.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.Count);
            Assert.True(result.TryGetValue("Order-1", out var order1Snapshot));
            Assert.Same(snapshot1, order1Snapshot);
            Assert.True(result.TryGetValue("Order-5", out var order5Snapshot));
            Assert.Same(snapshot5, order5Snapshot);
            Assert.False(result.ContainsKey("Order-2"));
            Assert.False(result.ContainsKey("Order-3"));
            Assert.False(result.ContainsKey("Order-4"));
        }

        [Fact]
        public async Task get_many_async_should_log_when_read_fails()
        {
            // Arrange
            var loggerMock = new Mock<INStoreLogger>();
            var loggerFactoryMock = new Mock<INStoreLoggerFactory>();
            loggerFactoryMock
                .Setup(f => f.CreateLogger(It.IsAny<string>()))
                .Returns(loggerMock.Object);

            var store = new ThrowingSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store, loggerFactoryMock.Object);

            // Act
            var result = await batchStore.GetManyAsync(new[] { "Order-1" }, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            loggerMock.Verify(l => l.LogError(It.IsAny<string>(), It.IsAny<object[]>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task get_many_async_should_respect_cancellation_token()
        {
            // Arrange
            using (var cts = new CancellationTokenSource())
            {
                cts.Cancel(); // Cancel immediately

                var store = new FakeSnapshotStore();
                await using var batchStore = new DefaultSnapshotBatchStore(store);
                var partitionIds = new[] { "Order-1" };

                // Act & Assert
                // The fake store doesn't actually check cancellation, but we verify the token is passed through
                // In a real implementation, this would throw OperationCanceledException
                await Assert.ThrowsAnyAsync<TaskCanceledException>(async () => await batchStore.GetManyAsync(partitionIds, cts.Token));
            }
        }

        [Fact]
        public async Task add_many_async_should_log_when_write_fails()
        {
            // Arrange
            var loggerMock = new Mock<INStoreLogger>();
            var loggerFactoryMock = new Mock<INStoreLoggerFactory>();
            loggerFactoryMock
                .Setup(f => f.CreateLogger(It.IsAny<string>()))
                .Returns(loggerMock.Object);

            var store = new ThrowingSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store, loggerFactoryMock.Object);

            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", new SnapshotInfo("Order-1", 1, "payload", "v1") }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            loggerMock.Verify(l => l.LogError(It.IsAny<string>(), It.IsAny<object[]>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task get_many_async_should_return_all_requested_snapshots_when_all_exist()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshots = Enumerable.Range(1, 100)
                .Select(i => new SnapshotInfo($"Order-{i}", i * 10, $"payload{i}", "v1"))
                .ToList();

            foreach (var snapshot in snapshots)
            {
                store.AddSnapshot(snapshot.SourceId, snapshot);
            }

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            var partitionIds = snapshots.Select(s => s.SourceId).ToArray();

            // Act
            var result = await batchStore.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert
            Assert.Equal(100, result.Count);
            foreach (var snapshot in snapshots)
            {
                Assert.True(result.TryGetValue(snapshot.SourceId, out var storedSnapshot));
                Assert.Same(snapshot, storedSnapshot);
            }
        }

        [Fact]
        public async Task extension_method_should_work_without_cancellation_token()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var snapshot = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            store.AddSnapshot("Order-1", snapshot);

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            var partitionIds = new[] { "Order-1" };

            // Act - Use the extension method without cancellation token
            var result = await batchStore.GetManyAsync(partitionIds);

            // Assert
            Assert.Single(result);
            Assert.Same(snapshot, result["Order-1"]);
        }

        // AddManyAsync tests
        [Fact]
        public async Task add_many_async_should_throw_when_snapshots_is_null()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                async () => await batchStore.AddManyAsync(null, CancellationToken.None)
            );
        }

        [Fact]
        public async Task add_many_async_should_handle_empty_dictionary()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            var snapshots = new Dictionary<string, SnapshotInfo>();

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(0, store.AddAsyncCallCount);
        }

        [Fact]
        public async Task add_many_async_should_save_all_valid_snapshots()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot2 = new SnapshotInfo("Order-2", 10, "payload2", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");
            
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", snapshot1 },
                { "Order-2", snapshot2 },
                { "Order-3", snapshot3 }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(3, store.AddAsyncCallCount);
            var result1 = await store.GetLastAsync("Order-1", CancellationToken.None);
            var result2 = await store.GetLastAsync("Order-2", CancellationToken.None);
            var result3 = await store.GetLastAsync("Order-3", CancellationToken.None);
            Assert.Same(snapshot1, result1);
            Assert.Same(snapshot2, result2);
            Assert.Same(snapshot3, result3);
        }

        [Fact]
        public async Task add_many_async_should_persist_original_values_when_input_is_mutated_after_enqueue()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            var originalSnapshot = new SnapshotInfo("Order-1", 5, "payload-original", "v1");
            var mutatedSnapshot = new SnapshotInfo("Order-1", 6, "payload-mutated", "v2");
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", originalSnapshot }
            };

            // Act: enqueue, then mutate caller-owned dictionary before queue flush
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            snapshots["Order-1"] = mutatedSnapshot;
            snapshots["Order-2"] = new SnapshotInfo("Order-2", 10, "payload-new", "v1");

            await batchStore.DisposeAsync();

            // Assert: persisted values come from the enqueue-time copy
            Assert.Equal(1, store.AddAsyncCallCount);
            var persistedOrder1 = await store.GetLastAsync("Order-1", CancellationToken.None);
            var persistedOrder2 = await store.GetLastAsync("Order-2", CancellationToken.None);
            Assert.Same(originalSnapshot, persistedOrder1);
            Assert.Null(persistedOrder2);
        }

        [Fact]
        public async Task add_many_async_should_use_batch_writer_when_available()
        {
            // Arrange
            var store = new BatchCapableSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", new SnapshotInfo("Order-1", 5, "payload1", "v1") },
                { "Order-2", new SnapshotInfo("Order-2", 10, "payload2", "v1") },
                { "Order-3", new SnapshotInfo("Order-3", 15, "payload3", "v1") }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(1, store.AddManyAsyncCallCount);
            Assert.Equal(0, store.AddAsyncCallCount);
            Assert.NotNull(await store.GetLastAsync("Order-1", CancellationToken.None));
            Assert.NotNull(await store.GetLastAsync("Order-2", CancellationToken.None));
            Assert.NotNull(await store.GetLastAsync("Order-3", CancellationToken.None));
        }

        [Fact]
        public async Task add_many_async_should_fallback_to_individual_writes_when_batch_writer_fails()
        {
            // Arrange
            var store = new BatchCapableSnapshotStore { ThrowOnAddMany = true };
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", new SnapshotInfo("Order-1", 5, "payload1", "v1") },
                { "Order-2", new SnapshotInfo("Order-2", 10, "payload2", "v1") }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(1, store.AddManyAsyncCallCount);
            Assert.Equal(2, store.AddAsyncCallCount);
            Assert.NotNull(await store.GetLastAsync("Order-1", CancellationToken.None));
            Assert.NotNull(await store.GetLastAsync("Order-2", CancellationToken.None));
        }

        [Fact]
        public async Task add_many_async_should_log_cancellation_from_batch_writer_during_flush()
        {
            // Arrange
            var store = new BatchCapableSnapshotStore { CancelOnAddMany = true };
            var loggerMock = new Mock<INStoreLogger>();
            var loggerFactoryMock = new Mock<INStoreLoggerFactory>();
            loggerFactoryMock
                .Setup(f => f.CreateLogger(It.IsAny<string>()))
                .Returns(loggerMock.Object);
            await using var batchStore = new DefaultSnapshotBatchStore(store, loggerFactoryMock.Object);

            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", new SnapshotInfo("Order-1", 5, "payload1", "v1") }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            Assert.Equal(1, store.AddManyAsyncCallCount);
            Assert.Equal(0, store.AddAsyncCallCount);
            loggerMock.Verify(l => l.LogError(It.IsAny<string>(), It.IsAny<object[]>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task add_many_async_should_filter_out_null_snapshots()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", snapshot1 },
                { "Order-2", null },
                { "Order-3", null }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(1, store.AddAsyncCallCount);
            var result = await store.GetLastAsync("Order-1", CancellationToken.None);
            Assert.Same(snapshot1, result);
        }

        [Fact]
        public async Task add_many_async_should_filter_out_empty_snapshots()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            
            var validSnapshot = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var emptySnapshot = new SnapshotInfo(null, 0, null, null); // Empty snapshot
            
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", validSnapshot },
                { "Order-2", emptySnapshot }
            };

            // Act
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(1, store.AddAsyncCallCount);
        }

        [Fact]
        public async Task add_many_async_should_not_throw_on_individual_failures()
        {
            // Arrange
            var store = new FailingSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot2 = new SnapshotInfo("fail", 10, "payload2", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");
            
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", snapshot1 },
                { "fail", snapshot2 },
                { "Order-3", snapshot3 }
            };

            // Act - Should not throw despite individual failure
            await batchStore.AddManyAsync(snapshots, CancellationToken.None);
            await batchStore.DisposeAsync();

            // Assert - Verify method completed without exception
            Assert.True(true);
        }

        [Fact]
        public async Task add_many_extension_method_should_work_without_cancellation_token()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            
            var snapshot = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", snapshot }
            };

            // Act - Use extension method without cancellation token
            await batchStore.AddManyAsync(snapshots);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(1, store.AddAsyncCallCount);
        }

        [Fact]
        public async Task add_many_async_should_throw_operation_cancelled_exception_when_token_is_cancelled()
        {
            // Arrange
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot2 = new SnapshotInfo("Order-2", 10, "payload2", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");
            
            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", snapshot1 },
                { "cancel", snapshot2 },
                { "Order-3", snapshot3 }
            };

            // Act & Assert - Should throw OperationCanceledException
            await Assert.ThrowsAsync<OperationCanceledException>(
                async () => await batchStore.AddManyAsync(snapshots, cts.Token)
            );
        }

        [Fact]
        public async Task add_many_async_should_be_thread_safe_under_concurrent_enqueues()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);

            var writes = Enumerable.Range(1, 100)
                .Select(i => batchStore.AddManyAsync(
                    new Dictionary<string, SnapshotInfo>
                    {
                        { $"Order-{i}", new SnapshotInfo($"Order-{i}", i, $"payload{i}", "v1") }
                    },
                    CancellationToken.None))
                .ToArray();

            // Act
            await Task.WhenAll(writes);
            await batchStore.DisposeAsync();

            // Assert
            Assert.Equal(100, store.AddAsyncCallCount);
        }

        [Fact]
        public async Task add_many_async_should_throw_after_dispose()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            await using var batchStore = new DefaultSnapshotBatchStore(store);
            await batchStore.DisposeAsync();

            var snapshots = new Dictionary<string, SnapshotInfo>
            {
                { "Order-1", new SnapshotInfo("Order-1", 1, "payload", "v1") }
            };

            // Act & Assert
            await Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await batchStore.AddManyAsync(snapshots, CancellationToken.None));
        }

        [Fact]
        public async Task add_many_async_should_flush_all_successfully_enqueued_items_when_dispose_races_with_writers()
        {
            // Arrange
            var store = new FakeSnapshotStore();
            var batchStore = new DefaultSnapshotBatchStore(store);
            var startGate = new ManualResetEventSlim(false);

            var writeTasks = Enumerable.Range(1, 200)
                .Select(i => Task.Run(async () =>
                {
                    startGate.Wait();

                    try
                    {
                        await batchStore.AddManyAsync(
                            new Dictionary<string, SnapshotInfo>
                            {
                                { $"Order-{i}", new SnapshotInfo($"Order-{i}", i, $"payload-{i}", "v1") }
                            },
                            CancellationToken.None).ConfigureAwait(false);
                        return true;
                    }
                    catch (ObjectDisposedException)
                    {
                        // Expected for writes that lose the race after dispose has started.
                        return false;
                    }
                }))
                .ToArray();

            // Act: start writers and begin disposal concurrently
            startGate.Set();
            var disposeTask = batchStore.DisposeAsync().AsTask();
            var enqueueResults = await Task.WhenAll(writeTasks).ConfigureAwait(false);
            await disposeTask.ConfigureAwait(false);

            // Assert: every successfully enqueued write is flushed exactly once
            var successfulEnqueues = enqueueResults.Count(x => x);
            Assert.Equal(successfulEnqueues, store.AddAsyncCallCount);
        }

        private class FailingSnapshotStore : ISnapshotStore
        {
            public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
            {
                return Task.FromResult<SnapshotInfo>(null);
            }

            public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }

            public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
            {
                if (snapshotPartitionId == "fail")
                    throw new InvalidOperationException("Simulated failure");

                return Task.CompletedTask;
            }

            public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public async Task get_many_async_should_return_partial_results_on_individual_failures()
        {
            // Arrange
            var store = new FailingOnGetSnapshotStore();
            var snapshot1 = new SnapshotInfo("Order-1", 5, "payload1", "v1");
            var snapshot3 = new SnapshotInfo("Order-3", 15, "payload3", "v1");
            store.AddSnapshot("Order-1", snapshot1);
            store.AddSnapshot("Order-3", snapshot3);

            await using var batchStore = new DefaultSnapshotBatchStore(store);
            // "fail" will throw, but Order-1 and Order-3 should succeed
            var partitionIds = new[] { "Order-1", "fail", "Order-3" };

            // Act - Should not throw despite individual failure
            var result = await batchStore.GetManyAsync(partitionIds, CancellationToken.None);

            // Assert - Should return partial results (Order-1 and Order-3)
            Assert.Equal(2, result.Count);
            Assert.Same(snapshot1, result["Order-1"]);
            Assert.Same(snapshot3, result["Order-3"]);
            Assert.False(result.ContainsKey("fail"));
        }

        private class FailingOnGetSnapshotStore : ISnapshotStore
        {
            private readonly Dictionary<string, SnapshotInfo> _snapshots = new Dictionary<string, SnapshotInfo>();

            public void AddSnapshot(string partitionId, SnapshotInfo snapshot)
            {
                _snapshots[partitionId] = snapshot;
            }

            public Task<SnapshotInfo> GetLastAsync(string snapshotPartitionId, CancellationToken cancellationToken)
            {
                if (snapshotPartitionId == "fail")
                    throw new InvalidOperationException("Simulated failure");

                _snapshots.TryGetValue(snapshotPartitionId, out var snapshot);
                return Task.FromResult(snapshot);
            }

            public Task<SnapshotInfo> GetAsync(string snapshotPartitionId, long version, CancellationToken cancellationToken)
                => throw new NotImplementedException();

            public Task AddAsync(string snapshotPartitionId, SnapshotInfo snapshot, CancellationToken cancellationToken)
                => Task.CompletedTask;

            public Task DeleteAsync(string snapshotPartitionId, long fromVersionInclusive, long toVersionInclusive, CancellationToken cancellationToken)
                => throw new NotImplementedException();
        }

    }
}
