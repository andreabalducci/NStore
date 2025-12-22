using System;
using System.Threading;
using System.Threading.Tasks;
using NStore.Core.Persistence;

namespace NStore.Core.Streams
{
    //@@REVIEW: could be refactored as a Decorator on Stream?
    public class OptimisticConcurrencyStream : IStream
    {
        private IPersistence Persistence { get; }

        private long _version = -1;
        public string Id { get; }
        public bool IsWritable => true;

        public OptimisticConcurrencyStream(
            string streamId,
            IPersistence persistence
        )
        {
            Id = streamId;
            Persistence = persistence;
        }

        public void MarkAsNew()
        {
            _version = 0;
        }

        public Task ReadAsync(
            ISubscription subscription,
            long fromIndexInclusive,
            long toIndexInclusive,
            CancellationToken cancellationToken
        )
        {
            // @@REVIEW: micro optimization for reading only last index? (fromIndexInclusive == toIndexInclusive == Int32.MaxValue)
            var readConsumer = subscription;
            if (toIndexInclusive == long.MaxValue)
            {
                _version = 0;
                readConsumer = new SubscriptionWrapper(subscription)
                {
                    BeforeOnNext = (data) => { _version = data.Index; }
                };
            }

            return Persistence.ReadForwardAsync(
                Id,
                fromIndexInclusive,
                readConsumer,
                toIndexInclusive,
                limit: Int32.MaxValue,
                cancellationToken: cancellationToken
            );
        }

        public async Task<IChunk> PeekAsync(CancellationToken cancellationToken)
        {
            var chunk = await Persistence.ReadSingleBackwardAsync(Id, cancellationToken).ConfigureAwait(false);
            _version = chunk?.Index ?? 0; //if we have no chunk version is zero.
            return chunk;
        }

        public async Task<bool> IsEmpty(CancellationToken cancellationToken)
        {
            // @@REVIEW: check version to avoid db roundtrip
            return (await Persistence.ReadSingleBackwardAsync(Id, cancellationToken)
                       .ConfigureAwait(false)) != null;
        }

        public async Task<bool> ContainsOperationAsync(
            string operationId,
            CancellationToken cancellationToken
        )
        {
            var chunk = await Persistence.ReadByOperationIdAsync(
                Id,
                operationId,
                cancellationToken
            ).ConfigureAwait(false);

            return chunk != null;
        }

        public async Task<IChunk> AppendAsync(
            object payload,
            string operationId,
            CancellationToken cancellation
        )
        {
            IChunk chunk = null;
            if (_version == -1)
            {
                throw new AppendFailedException(Id,
                    $@"Cannot append on stream {Id}
Append can be called only after a Read operation.
If you don't need to read use {nameof(Stream)} instead of {GetType().Name}.");
            }

            long desiredVersion = _version + 1;
            try
            {
                chunk = await Persistence.AppendAsync(
                    Id,
                    desiredVersion,
                    payload,
                    operationId,
                    cancellation
                ).ConfigureAwait(false);
            }
            catch (DuplicateStreamIndexException e)
            {
                throw new ConcurrencyException($"Concurrency exception on StreamId: {this.Id}", e);
            }

            _version = desiredVersion;
            return chunk;
        }

        public Task DeleteAsync(CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(Id, 0, long.MaxValue, cancellation);
        }

        public Task DeleteBeforeAsync(long index, CancellationToken cancellation)
        {
            return Persistence.DeleteAsync(Id, 0, index-1, cancellation);
        }
    }
}