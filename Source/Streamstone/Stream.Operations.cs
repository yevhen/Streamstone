using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Streamstone.Utility;

namespace Streamstone
{
    public sealed partial class Stream
    {
        class ProvisionOperation
        {
            readonly Stream stream;
            readonly TableClient table;

            public ProvisionOperation(Stream stream)
            {
                Debug.Assert(stream.IsTransient);
                
                this.stream = stream;
                table = stream.Partition.Table;
            }

            public async Task<Stream> ExecuteAsync()
            {
                var insert = new Insert(stream);
                Response response = null;

                try
                {
                    response = await insert.ExecuteAsync(table).ConfigureAwait(false);
                }
                catch (RequestFailedException e)
                {
                    insert.Handle(e);
                }

                return insert.Result(response);
            }

            class Insert
            {
                readonly StreamEntity entity;
                readonly Partition partition;

                public Insert(Stream stream)
                {
                    entity = stream.ToStreamEntity();
                    partition = stream.Partition;
                }

                internal Task<Response> ExecuteAsync(TableClient table)
                {
                    return table.AddEntityAsync(entity.ToTableEntity());
                }

                internal void Handle(RequestFailedException exception)
                {
                    if (exception.ErrorCode == TableErrorCode.EntityAlreadyExists)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    ExceptionDispatchInfo.Capture(exception).Throw();
                }

                internal Stream Result(Response response)
                {
                    entity.ETag = response.Headers.ETag.Value;
                    return From(partition, entity);
                }
            }
        }

        class WriteOperation
        {
            const int MaxOperationsPerChunk = 99;

            readonly Stream stream;
            readonly StreamWriteOptions options;
            readonly TableClient table;
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
                    Response<IReadOnlyList<Response>> response = null;

                    try
                    {
                        response = await table.SubmitTransactionAsync(batch.Prepare()).ConfigureAwait(false);
                    }
                    catch (TableTransactionFailedException e)
                    {
                        batch.Handle(e);
                    }

                    current = batch.Result(response);
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
                { }

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
                    var entity = stream.ToStreamEntity();
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

                internal IEnumerable<TableTransactionAction> Prepare()
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

                IEnumerable<TableTransactionAction> ToBatch()
                {
                    return operations.Select(each => (TableTransactionAction)each);
                }

                internal Stream Result(Response<IReadOnlyList<Response>> response)
                {
                    for (var i = 0; i < operations.Count; i++)
                    {
                        var etag = response.Value[i].Headers.ETag;

                        if (etag.HasValue)
                            operations[i].Entity.ETag = etag.Value;
                    }

                    return From(partition, stream);
                }

                internal void Handle(TableTransactionFailedException exception)
                {
                    if (exception.ErrorCode == TableErrorCode.UpdateConditionNotSatisfied)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    if (exception.ErrorCode != TableErrorCode.EntityAlreadyExists)
                        ExceptionDispatchInfo.Capture(exception).Throw();

                    var conflicting = operations[exception.FailedTransactionActionIndex.Value].Entity;

                    if (conflicting == stream)
                        throw ConcurrencyConflictException.StreamChangedOrExists(partition);

                    if (conflicting is EventIdEntity id)
                        throw new DuplicateEventException(partition, id.Event.Id);

                    if (conflicting is EventEntity @event)
                        throw ConcurrencyConflictException.EventVersionExists(partition, @event.Version);

                    var include = operations.Single(x => x.Entity == conflicting);
                    throw IncludedOperationConflictException.Create(partition, include);
                }
            }
        }

        class SetPropertiesOperation
        {
            readonly Stream stream;
            readonly TableClient table;
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
                Response response = null;

                try
                {
                    response = await replace.ExecuteAsync(table).ConfigureAwait(false);
                }
                catch (RequestFailedException e)
                {
                    replace.Handle(e);
                }

                return replace.Result(response);
            }

            class Replace
            {
                readonly StreamEntity stream;
                readonly Partition partition;

                public Replace(Stream stream, StreamProperties properties)
                {
                    this.stream = stream.ToStreamEntity(properties);
                    partition = stream.Partition;
                }

                internal Task<Response> ExecuteAsync(TableClient table)
                {
                    return table.UpdateEntityAsync(stream.ToTableEntity(), stream.ETag, TableUpdateMode.Replace);
                }

                internal void Handle(RequestFailedException exception)
                {
                    if (exception.ErrorCode == TableErrorCode.UpdateConditionNotSatisfied)
                        throw ConcurrencyConflictException.StreamChanged(partition);

                    ExceptionDispatchInfo.Capture(exception).Throw();
                }

                internal Stream Result(Response response)
                {
                    stream.ETag = response.Headers.ETag.Value;
                    return From(partition, stream);
                }
            }
        }

        class OpenStreamOperation
        {
            readonly Partition partition;
            readonly TableClient table;

            public OpenStreamOperation(Partition partition)
            {
                this.partition = partition;
                table = partition.Table;
            }

            public async Task<StreamOpenResult> ExecuteAsync()
            {
                var entity = await table.GetEntityIfExistsAsync<TableEntity>(partition.PartitionKey, partition.StreamRowKey());

                if (!entity.HasValue)
                    return StreamOpenResult.NotFound;

                return new StreamOpenResult(true, From(partition, entity.Value));
            }
        }

        class ReadOperation<T> where T : class
        {
            readonly Partition partition;
            readonly TableClient table;

            readonly long startVersion;
            readonly long sliceSize;

            public ReadOperation(Partition partition, long startVersion, long sliceSize)
            {
                this.partition = partition;
                this.startVersion = startVersion;
                this.sliceSize = sliceSize;
                table = partition.Table;
            }

            public async Task<StreamSlice<T>> ExecuteAsync(Func<TableEntity, T> transform)
            {
                var eventsQuery = ExecuteQueryAsync(EventsQuery());
                var streamRowQuery = ExecuteQueryAsync(StreamRowQuery());
                await Task.WhenAll(eventsQuery, streamRowQuery);
                return Result(await eventsQuery, FindStreamEntity(await streamRowQuery), transform);
            }

            StreamSlice<T> Result(ICollection<TableEntity> entities, TableEntity streamEntity, Func<TableEntity, T> transform)
            {
                var stream = BuildStream(streamEntity);
                var events = BuildEvents(entities, transform);

                return new StreamSlice<T>(stream, events, startVersion, sliceSize);
            }

            string EventsQuery()
            {
                var rowKeyStart = partition.EventVersionRowKey(startVersion);
                var rowKeyEnd = partition.EventVersionRowKey(startVersion + sliceSize - 1);

                return TableClient.CreateQueryFilter($"PartitionKey eq {partition.PartitionKey} and RowKey ge {rowKeyStart} and RowKey le {rowKeyEnd}");
            }

            string StreamRowQuery()
            {
                return TableClient.CreateQueryFilter($"PartitionKey eq {partition.PartitionKey} and RowKey eq {partition.StreamRowKey()}");
            }

            async Task<List<TableEntity>> ExecuteQueryAsync(string query)
            {
                var list = new List<TableEntity>();

                await foreach (var entity in table.QueryAsync<TableEntity>(query))
                    list.Add(entity);

                return list;
            }

            TableEntity FindStreamEntity(IEnumerable<TableEntity> entities)
            {
                var result = entities.SingleOrDefault(x => x.RowKey == partition.StreamRowKey());

                if (result == null)
                    throw new StreamNotFoundException(partition);

                return result;
            }

            Stream BuildStream(TableEntity entity) => From(partition, StreamEntity.From(entity));

            static T[] BuildEvents(IEnumerable<TableEntity> entities, Func<TableEntity, T> transform) =>
                entities.Select(transform).ToArray();
        }
    }
}