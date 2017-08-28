﻿using System;
using System.Collections.Generic;
using System.Linq;
using NStore.Processing;
using NStore.Snapshots;

namespace NStore.Aggregates
{
    public abstract class Aggregate<TState> :
        IEventSourcedAggregate,
        ISnaphottable,
        IAggregate
        where TState : AggregateState, new()
    {
        public string Id { get; private set; }
        public long Version { get; private set; }
        public bool IsInitialized { get; private set; }

        private IList<object> PendingChanges { get; } = new List<object>();
        protected TState State { get; private set; }
        public bool IsDirty => this.PendingChanges.Any();
        public bool IsNew => this.Version == 0;
        private readonly IPayloadProcessor _processor;

        protected Aggregate() : this(null)
        {
            
        }

        protected Aggregate(IPayloadProcessor processor)
        {
            _processor = processor ?? DelegateToPrivateEventHandlers.Instance;
        }

        protected virtual string StateSignature => "1";
        
        public void Init(string id) => InternalInit(id, 0, null);

        private void InternalInit(string aggregateId, long aggregateVersion, TState state)
        {
            if (String.IsNullOrEmpty(aggregateId))
                throw new ArgumentNullException(nameof(aggregateId));

            if (this.Id != null)
                throw new AggregateAlreadyInitializedException(GetType(), this.Id);

            this.Id = aggregateId;
            this.State = state ?? new TState();
            this.IsInitialized = true;
            this.PendingChanges.Clear();
            this.Version = aggregateVersion;
        }

        bool ISnaphottable.TryRestore(SnapshotInfo snapshotInfo)
        {
            if (snapshotInfo == null) throw new ArgumentNullException(nameof(snapshotInfo));

            var processed = PreprocessSnapshot(snapshotInfo);

            if (processed == null || processed.IsEmpty)
                return false;

            this.InternalInit(
                processed.SourceId,
                processed.SourceVersion,
                (TState)processed.Payload
            );

            return true;
        }

        protected virtual void PostLoadingProcessing()
        {
            // entry point for custom logic on load
        }

        /// <summary>
        /// Give chance to upcast state or just drop snapshot with empty default state
        /// </summary>
        /// <param name="snapshotInfo"></param>
        /// <returns></returns>
        protected virtual SnapshotInfo PreprocessSnapshot(SnapshotInfo snapshotInfo)
        {
            return snapshotInfo;
        }

        SnapshotInfo ISnaphottable.GetSnapshot()
        {
            return new SnapshotInfo(
                this.Id,
                this.Version,
                this.State,
                this.StateSignature
            );
        }

        void IEventSourcedAggregate.Loaded()
        {
            PostLoadingProcessing();
        }

        void IEventSourcedAggregate.Persisted(Changeset changeset)
        {
            this.Version = changeset.AggregateVersion;
            this.PendingChanges.Clear();
        }

        void IEventSourcedAggregate.ApplyChanges(Changeset changeset)
        {
            // skip if same version
            if (changeset.AggregateVersion == this.Version)
                return;

            if (changeset.AggregateVersion != this.Version + 1)
                throw new AggregateRestoreException(this.Version + 1, changeset.AggregateVersion);

            this.Version = changeset.AggregateVersion;
            this._processor.FoldEach(this.State, changeset.Events);
        }

        Changeset IEventSourcedAggregate.GetChangeSet()
        {
            return new Changeset(
                this.Version + 1,
                this.PendingChanges.ToArray()
            );
        }

        protected void Emit(object @event)
        {
            this.PendingChanges.Add(@event);
            this._processor.Process(this.State, @event);
        }
    }
}