using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        class ProvisionOperation
        {
            readonly Stream stream;
            readonly CloudTable table;

            public ProvisionOperation(Stream stream)
            {
                Debug.Assert(stream.IsTransient);
                
                this.stream = stream;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync()
            {
                var insert = new Insert(stream);

                try
                {
                    await table.ExecuteAsync(insert.Prepare()).ConfigureAwait(false);
                }
                catch (StorageException e)
                {
                    insert.Handle(e);
                }

                return insert.Result();
            }

            class Insert
            {
                readonly StreamEntity stream;
                readonly Partition partition;

                public Insert(Stream stream)
                {
                    this.stream = stream.Entity();
                    partition = stream.Partition;
                }

                public TableOperation Prepare() => TableOperation.Insert(stream);

                internal void Handle(StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    ExceptionDispatchInfo.Capture(exception).Throw();
                }

                internal Stream Result() => From(partition, stream);
            }
        }

        class WriteOperation
        {
            const int MaxOperationsPerChunk = 99;

            readonly Stream stream;
            readonly StreamWriteOptions options;
            readonly CloudTable table;
            readonly IEnumerable<RecordedEvent> events;

            public WriteOperation(Stream stream, StreamWriteOptions options, IEnumerable<EventData> events)
            {
                this.stream = stream;
                this.options = options;
                this.events = stream.Record(events);
                table = stream.Partition.Table;
            }

            public async Task<StreamWriteResult> ExecuteAsync()
            {
                var current = stream;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current, options);

                    try
                    {
                        await table.ExecuteBatchAsync(batch.Prepare()).ConfigureAwait(false);
                    }
                    catch (StorageException e)
                    {
                        batch.Handle(e);
                    }

                    current = batch.Result();
                }

                return new StreamWriteResult(current, events.ToArray());
            }

            IEnumerable<Chunk> Chunks() => Chunk.Split(events).Where(s => !s.IsEmpty);

            class Chunk
            {
                public static IEnumerable<Chunk> Split(IEnumerable<RecordedEvent> events)
                {
                    var current = new Chunk();

                    foreach (var @event in events)
                    {
                        var next = current.Add(@event);

                        if (next != current)
                            yield return current;

                        current = next;
                    }

                    yield return current;
                }

                readonly List<RecordedEvent> events = new List<RecordedEvent>();
                int operations;

                Chunk()
                {}

                Chunk(RecordedEvent first) => Accomodate(first);

                Chunk Add(RecordedEvent @event)
                {
                    if (@event.Operations > MaxOperationsPerChunk)
                        throw new InvalidOperationException(
                            string.Format("{0} include(s) in event {1}:{{{2}}}, plus event entity itself, is over Azure's max batch size limit [{3}]",
                                          @event.IncludedOperations.Length, @event.Version, @event.Id, MaxOperationsPerChunk));
                    
                    if (!CanAccomodate(@event))
                        return new Chunk(@event);

                    Accomodate(@event);
                    return this;
                }

                void Accomodate(RecordedEvent @event)
                {
                    operations += @event.Operations;
                    events.Add(@event);
                }

                bool CanAccomodate(RecordedEvent @event)
                {
                    return operations + @event.Operations <= MaxOperationsPerChunk;
                }

                public bool IsEmpty => events.Count == 0;

                public Batch ToBatch(Stream stream, StreamWriteOptions options)
                {
                    var entity = stream.Entity();
                    entity.Version += events.Count; 
                    return new Batch(entity, events, options);
                }
            }

            class Batch
            {
                readonly List<EntityOperation> operations = 
                     new List<EntityOperation>();
                
                readonly StreamEntity stream;
                readonly List<RecordedEvent> events;
                readonly StreamWriteOptions options;
                readonly Partition partition;

                internal Batch(StreamEntity stream, List<RecordedEvent> events, StreamWriteOptions options)
                {
                    this.stream = stream;
                    this.events = events;
                    this.options = options;
                    partition = stream.Partition;
                }

                internal TableBatchOperation Prepare()
                {
                    WriteStream();
                    WriteEvents();
                    WriteIncludes();

                    return ToBatch();
                }

                void WriteStream() => operations.Add(stream.Operation());

                void WriteEvents() => operations.AddRange(events.SelectMany(e => e.EventOperations));

                void WriteIncludes()
                {
                    if (!options.TrackChanges)
                    {
                        operations.AddRange(events.SelectMany(x => x.IncludedOperations));
                        return;
                    }
                    
                    var tracker = new EntityChangeTracker();

                    foreach (var @event in events)
                        tracker.Record(@event.IncludedOperations);

                    operations.AddRange(tracker.Compute());
                }

                TableBatchOperation ToBatch()
                {
                    var result = new TableBatchOperation();
                    
                    foreach (var each in operations)
                        result.Add(each);

                    return result;
                }

                internal Stream Result()
                {
                    return From(partition, stream);
                }

                internal void Handle(StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    if (exception.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        ExceptionDispatchInfo.Capture(exception).Throw();

                    var error = exception.RequestInformation.ExtendedErrorInformation;
                    if (error.ErrorCode != "EntityAlreadyExists")
                        throw UnexpectedStorageResponseException.ErrorCodeShouldBeEntityAlreadyExists(error);

                    var position = ParseConflictingEntityPosition(error);

                    Debug.Assert(position >= 0 && position < operations.Count);
                    var conflicting = operations[position].Entity;

                    if (conflicting == stream)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    var id = conflicting as EventIdEntity;
                    if (id != null)
                        throw new DuplicateEventException(partition, id.Event.Id);

                    var @event = conflicting as EventEntity;
                    if (@event != null)
                        throw ConcurrencyConflictException.EventVersionExists(partition, @event.Version);

                    var include = operations.Single(x => x.Entity == conflicting); 
                    throw IncludedOperationConflictException.Create(partition, include);
                }

                static int ParseConflictingEntityPosition(StorageExtendedErrorInformation error)
                {
                    var lines = error.ErrorMessage.Split('\n');
                    if (lines.Length != 3)
                        throw UnexpectedStorageResponseException.ConflictExceptionMessageShouldHaveExactlyThreeLines(error);

                    var semicolonIndex = lines[0].IndexOf(":", StringComparison.Ordinal);
                    if (semicolonIndex == -1)
                        throw UnexpectedStorageResponseException.ConflictExceptionMessageShouldHaveSemicolonOnFirstLine(error);

                    int position;
                    if (!int.TryParse(lines[0].Substring(0, semicolonIndex), out position))
                        throw UnexpectedStorageResponseException.UnableToParseTextBeforeSemicolonToInteger(error);

                    return position;
                }
            }
        }

        class SetPropertiesOperation
        {
            readonly Stream stream;
            readonly CloudTable table;
            readonly StreamProperties properties;

            public SetPropertiesOperation(Stream stream, StreamProperties properties)
            {                
                this.stream = stream;
                this.properties = properties;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync()
            {
                var replace = new Replace(stream, properties);

                try
                {
                    await table.ExecuteAsync(replace.Prepare()).ConfigureAwait(false);
                }
                catch (StorageException e)
                {
                    replace.Handle(e);
                }

                return replace.Result();
            }

            class Replace
            {
                readonly StreamEntity stream;
                readonly Partition partition;

                public Replace(Stream stream, StreamProperties properties)
                {
                    this.stream = stream.Entity();
                    this.stream.Properties = properties;
                    partition = stream.Partition;
                }

                internal TableOperation Prepare()
                {                    
                    return TableOperation.Replace(stream);
                }

                internal void Handle(StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                        throw ConcurrencyConflictException.StreamChanged(partition);

                    ExceptionDispatchInfo.Capture(exception).Throw();
                }

                internal Stream Result() => From(partition, stream);
            }
        }

        class OpenStreamOperation
        {
            readonly Partition partition;
            readonly CloudTable table;

            public OpenStreamOperation(Partition partition)
            {
                this.partition = partition;
                table = partition.Table;
            }

            public async Task<StreamOpenResult> ExecuteAsync() => 
                Result(await table.ExecuteAsync(Prepare()));

            TableOperation Prepare() => TableOperation.Retrieve<StreamEntity>(partition.PartitionKey, partition.StreamRowKey());

            StreamOpenResult Result(TableResult result)
            {
                var entity = result.Result;

                return entity != null
                           ? new StreamOpenResult(true, From(partition, (StreamEntity)entity))
                           : StreamOpenResult.NotFound;
            }
        }

        class ReadOperation<T> where T : class
        {
            readonly Partition partition;
            readonly CloudTable table;

            readonly int startVersion;
            readonly int sliceSize;

            public ReadOperation(Partition partition, int startVersion, int sliceSize)
            {
                this.partition = partition;
                this.startVersion = startVersion;
                this.sliceSize = sliceSize;
                table = partition.Table;
            }

            public async Task<StreamSlice<T>> ExecuteAsync(Func<DynamicTableEntity, T> transform) => 
                Result(await ExecuteQueryAsync(PrepareQuery()), transform);

            StreamSlice<T> Result(ICollection<DynamicTableEntity> entities, Func<DynamicTableEntity, T> transform)
            {
                var streamEntity = FindStreamEntity(entities);
                entities.Remove(streamEntity);

                var stream = BuildStream(streamEntity);
                var events = BuildEvents(entities, transform);

                return new StreamSlice<T>(stream, events, startVersion, sliceSize);
            }

            TableQuery<DynamicTableEntity> PrepareQuery()
            {
                var rowKeyStart = partition.EventVersionRowKey(startVersion);
                var rowKeyEnd = partition.EventVersionRowKey(startVersion + sliceSize - 1);

                var filter = TableQuery.CombineFilters(
                   TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.PartitionKey), QueryComparisons.Equal, partition.PartitionKey),
                   TableOperators.And,
                       TableQuery.CombineFilters(
                           TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.Equal, partition.StreamRowKey()),
                           TableOperators.Or,
                           TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.GreaterThanOrEqual, rowKeyStart),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.LessThanOrEqual, rowKeyEnd)
                           )
                       )
               );

                return new TableQuery<DynamicTableEntity>().Where(filter);
            }

            async Task<List<DynamicTableEntity>> ExecuteQueryAsync(TableQuery<DynamicTableEntity> query)
            {
                var result = new List<DynamicTableEntity>();
                TableContinuationToken token = null;

                do
                {
                    var segment = await table.ExecuteQuerySegmentedAsync(query, token).ConfigureAwait(false);
                    token = segment.ContinuationToken;
                    result.AddRange(segment.Results);
                }
                while (token != null);

                return result;
            }

            DynamicTableEntity FindStreamEntity(IEnumerable<DynamicTableEntity> entities)
            {
                var result = entities.SingleOrDefault(x => x.RowKey == partition.StreamRowKey());

                if (result == null)
                    throw new StreamNotFoundException(partition);

                return result;
            }

            Stream BuildStream(DynamicTableEntity entity) => From(partition, StreamEntity.From(entity));

            static T[] BuildEvents(IEnumerable<DynamicTableEntity> entities, Func<DynamicTableEntity, T> transform) => 
                entities.Select(transform).ToArray();
        }
    }
}
