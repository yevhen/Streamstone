using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
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

            public Stream Execute()
            {
                var insert = new Insert(stream);

                try
                {
                    table.Execute(insert.Prepare());
                }
                catch (StorageException e)
                {
                    insert.Handle(table, e);
                }

                return insert.Result();
            }

            public async Task<Stream> ExecuteAsync()
            {
                var insert = new Insert(stream);

                try
                {
                    await table.ExecuteAsync(insert.Prepare()).Really();
                }
                catch (StorageException e)
                {
                    insert.Handle(table, e);
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

                public TableOperation Prepare()
                {
                    return TableOperation.Insert(stream);
                }

                internal void Handle(CloudTable table, StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                        throw ConcurrencyConflictException.StreamChangedOrExists(table, partition);

                    throw exception.PreserveStackTrace();
                }

                internal Stream Result()
                {
                    return From(partition, stream);
                }
            }
        }

        class WriteOperation
        {
            const int MaxOperationsPerChunk = 99;

            readonly Stream stream;
            readonly CloudTable table;
            readonly IEnumerable<RecordedEvent> events;

            public WriteOperation(Stream stream, IEnumerable<EventData> events)
            {
                this.stream = stream;
                this.events = stream.Record(events);
                table = stream.Partition.Table;
            }

            public StreamWriteResult Execute()
            {
                var current = stream;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current);

                    try
                    {
                        table.ExecuteBatch(batch.Prepare());
                    }
                    catch (StorageException e)
                    {
                        batch.Handle(table, e);
                    }

                    current = batch.Result();
                }

                return new StreamWriteResult(current, events.ToArray());
            }

            public async Task<StreamWriteResult> ExecuteAsync()
            {
                var current = stream;

                foreach (var chunk in Chunks())
                {
                    var batch = chunk.ToBatch(current);

                    try
                    {
                        await table.ExecuteBatchAsync(batch.Prepare()).Really();
                    }
                    catch (StorageException e)
                    {
                        batch.Handle(table, e);
                    }

                    current = batch.Result();
                }

                return new StreamWriteResult(current, events.ToArray());
            }

            IEnumerable<Chunk> Chunks()
            {
                return Chunk.Split(events).Where(s => !s.IsEmpty);
            }

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

                Chunk(RecordedEvent first)
                {
                    Accomodate(first);
                }

                Chunk Add(RecordedEvent @event)
                {
                    if (@event.Operations.Length > MaxOperationsPerChunk)
                        throw new InvalidOperationException(
                            string.Format("{0} include(s) in event {1}:{{{2}}}, plus event entity itself, is over Azure's max batch size limit [{3}]",
                                          @event.Includes.Count(), @event.Version, @event.Id, MaxOperationsPerChunk));
                    
                    if (!CanAccomodate(@event))
                        return new Chunk(@event);

                    Accomodate(@event);
                    return this;
                }

                void Accomodate(RecordedEvent @event)
                {
                    operations += @event.Operations.Length;
                    events.Add(@event);
                }

                bool CanAccomodate(RecordedEvent @event)
                {
                    return operations + @event.Operations.Length <= MaxOperationsPerChunk;
                }

                public bool IsEmpty
                {
                    get { return events.Count == 0; }
                }

                public Batch ToBatch(Stream stream)
                {
                    var entity = stream.Entity();
                    entity.Version += events.Count; 
                    return new Batch(entity, events);
                }
            }

            class Batch
            {
                readonly List<EntityOperation> operations = 
                     new List<EntityOperation>();
                
                readonly StreamEntity stream;
                readonly IEnumerable<RecordedEvent> events;
                readonly Partition partition;

                internal Batch(StreamEntity stream, IEnumerable<RecordedEvent> events)
                {
                    this.stream = stream;
                    this.events = events;
                    partition = stream.Partition;
                }

                internal TableBatchOperation Prepare()
                {
                    WriteStream();
                    WriteEvents();

                    return ToBatch();
                }

                void WriteStream()
                {
                    operations.Add(stream.Operation());
                }

                void WriteEvents()
                {
                    foreach (var e in events)
                        WriteEvent(e);
                }

                void WriteEvent(RecordedEvent @event)
                {
                    operations.AddRange(@event.Operations);
                }

                TableBatchOperation ToBatch()
                {
                    var result = new TableBatchOperation();
                    
                    foreach (var each in operations)
                        result.Add(each.Operation);

                    return result;
                }

                internal Stream Result()
                {
                    return From(partition, stream);
                }

                internal void Handle(CloudTable table, StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                        throw ConcurrencyConflictException.StreamChangedOrExists(table, partition);

                    if (exception.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                        throw exception.PreserveStackTrace();

                    var error = exception.RequestInformation.ExtendedErrorInformation;
                    if (error.ErrorCode != "EntityAlreadyExists")
                        throw UnexpectedStorageResponseException.ErrorCodeShouldBeEntityAlreadyExists(error);

                    var position = ParseConflictingEntityPosition(error);

                    Debug.Assert(position >= 0 && position < operations.Count);
                    var conflicting = operations[position].Entity;

                    if (conflicting == stream)
                        throw ConcurrencyConflictException.StreamChangedOrExists(table, partition);

                    var id = conflicting as EventIdEntity;
                    if (id != null)
                        throw new DuplicateEventException(table, partition, id.Event.Id);

                    var @event = conflicting as EventEntity;
                    if (@event != null)
                        throw ConcurrencyConflictException.EventVersionExists(table, partition, @event.Version);

                    var include = events.SelectMany(e => e.Includes).SingleOrDefault(x => x.Entity == conflicting);
                    if (include != null)
                        throw new IncludedOperationConflictException(table, partition, include);

                    throw new WarningException("How did this happen? We've got conflict on entity which is neither event nor id or include");
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

            public Stream Execute()
            {
                var replace = new Replace(stream, properties);

                try
                {
                    table.Execute(replace.Prepare());
                }
                catch (StorageException e)
                {
                    replace.Handle(table, e);
                }

                return replace.Result();
            }

            public async Task<Stream> ExecuteAsync()
            {
                var replace = new Replace(stream, properties);

                try
                {
                    await table.ExecuteAsync(replace.Prepare()).Really();
                }
                catch (StorageException e)
                {
                    replace.Handle(table, e);
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

                internal void Handle(CloudTable table, StorageException exception)
                {
                    if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                        throw ConcurrencyConflictException.StreamChanged(table, partition);

                    throw exception.PreserveStackTrace();
                }

                internal Stream Result()
                {
                    return From(partition, stream);
                }
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

            public StreamOpenResult Execute()
            {
                return Result(table.Execute(Prepare()));
            }

            public async Task<StreamOpenResult> ExecuteAsync()
            {
                return Result(await table.ExecuteAsync(Prepare()));
            }

            TableOperation Prepare()
            {
                return TableOperation.Retrieve<StreamEntity>(partition.PartitionKey, partition.StreamRowKey());
            }

            StreamOpenResult Result(TableResult result)
            {
                var entity = result.Result;

                return entity != null
                           ? new StreamOpenResult(true, From(partition, (StreamEntity)entity))
                           : StreamOpenResult.NotFound;
            }
        }

        class ReadOperation<T> where T : class, new()
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

            public StreamSlice<T> Execute()
            {
                return Result(ExecuteQuery(PrepareQuery()));
            }

            public async Task<StreamSlice<T>> ExecuteAsync()
            {
                return Result(await ExecuteQueryAsync(PrepareQuery()));
            }

            StreamSlice<T> Result(ICollection<DynamicTableEntity> entities)
            {
                var streamEntity = FindStreamEntity(entities);
                entities.Remove(streamEntity);

                var stream = BuildStream(streamEntity);
                var events = BuildEvents(entities);

                return new StreamSlice<T>(stream, events, startVersion, sliceSize);
            }

            TableQuery<DynamicTableEntity> PrepareQuery()
            {
                var rowKeyStart = partition.EventVersionRowKey(startVersion);
                var rowKeyEnd = partition.EventVersionRowKey(startVersion + sliceSize - 1);

                // ReSharper disable StringCompareToIsCultureSpecific

                var query = table
                    .CreateQuery<DynamicTableEntity>()
                    .Where(x =>
                           x.PartitionKey == partition.PartitionKey
                           && (x.RowKey == partition.StreamRowKey()
                               || (x.RowKey.CompareTo(rowKeyStart)  >= 0
                                   && x.RowKey.CompareTo(rowKeyEnd) <= 0)));

                return (TableQuery<DynamicTableEntity>) query;
            }

            List<DynamicTableEntity> ExecuteQuery(TableQuery<DynamicTableEntity> query)
            {
                var result = new List<DynamicTableEntity>();
                TableContinuationToken token = null;

                do
                {
                    var segment = table.ExecuteQuerySegmented(query, token);
                    token = segment.ContinuationToken;
                    result.AddRange(segment.Results);
                }
                while (token != null);

                return result;
            }

            async Task<List<DynamicTableEntity>> ExecuteQueryAsync(TableQuery<DynamicTableEntity> query)
            {
                var result = new List<DynamicTableEntity>();
                TableContinuationToken token = null;

                do
                {
                    var segment = await table.ExecuteQuerySegmentedAsync(query, token).Really();
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
                {
                    throw new StreamNotFoundException(partition.Table, partition);
                }

                return result;
            }

            Stream BuildStream(DynamicTableEntity entity)
            {
                return From(partition, StreamEntity.From(entity));
            }

            static T[] BuildEvents(IEnumerable<DynamicTableEntity> entities)
            {
                return entities.Select(e => e.Properties).Select(properties =>
                {
                    var t = new T();
                    TableEntity.ReadUserObject(t, properties, new OperationContext());
                    return t;
                })
                .ToArray();
            }
        }
    }
}
