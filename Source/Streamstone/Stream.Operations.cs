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
            readonly CloudTable table;
            readonly Stream stream;

            public ProvisionOperation(CloudTable table, Stream stream)
            {
                this.table = table;
                this.stream = stream;
            }

            StreamEntity streamEntity;

            public Task ExecuteAsync()
            {
                streamEntity = stream.Entity();
                var insert = TableOperation.Insert(streamEntity);
                return table.ExecuteAsync(insert);
            }

            public Stream Result()
            {
                return From(streamEntity);
            }
        }

        class WriteOperation
        {
            readonly TableBatchOperation batch = new TableBatchOperation();
            readonly List<ITableEntity> entities = new List<ITableEntity>(); 
            
            readonly CloudTable table;
            readonly Stream stream;
            readonly Include[] includes;
            readonly WriteAttempt attempt;

            public WriteOperation(CloudTable table, Stream stream, Event[] events, Include[] includes)
            {
                this.table = table;
                this.stream = stream;
                this.includes = includes;

                attempt = this.stream.Write(events);
            }

            public Task ExecuteAsync()
            {
                PrepareBatch();
                return ExecuteBatch();
            }

            void PrepareBatch()
            {
                WriteStream();
                WriteEvents();
                WriteIncludes();
            }

            Task ExecuteBatch()
            {
                return table.ExecuteBatchAsync(batch);
            }

            void WriteStream()
            {
                var streamEntity = attempt.Stream.Entity();

                if (stream.Etag == null)
                    batch.Insert(streamEntity);
                else
                    batch.Replace(streamEntity);

                entities.Add(streamEntity);
            }

            void WriteEvents()
            {
                foreach (var e in attempt.Events)
                {
                    var eventEntity = e.EventEntity();
                    var eventIdEntity = e.IdEntity();

                    batch.Insert(eventEntity);
                    batch.Insert(eventIdEntity);

                    entities.Add(eventEntity);
                    entities.Add(eventIdEntity);
                }
            }

            void WriteIncludes()
            {
                foreach (var include in includes)
                {
                    batch.Add(include.Apply(stream.Partition));
                    entities.Add(include.Entity);
                }
            }

            public StreamWriteResult Result()
            {
                var streamEntity = entities.OfType<StreamEntity>().Single();

                var storedStream = From(streamEntity);
                var storedEvents = attempt.Events
                    .Select(e => new StoredEvent(e.Id, e.Version, e.Properties))
                    .ToArray();

                return new StreamWriteResult(storedStream, storedEvents);
            }

            public void Handle(StorageException exception)
            {
                if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    throw ConcurrencyConflictException.StreamChangedOrExists(table, stream.Partition);

                if (exception.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw exception.PreserveStackTrace();

                var error = exception.RequestInformation.ExtendedErrorInformation;
                if (error.ErrorCode != "EntityAlreadyExists")
                    throw UnexpectedStorageResponseException.ErrorCodeShouldBeEntityAlreadyExists(error);

                var position = ParseConflictingEntityPosition(error);
                Debug.Assert(position >= 0 && position < entities.Count);

                var conflicting = entities[position];
                if (conflicting is EventIdEntity)
                {
                    var duplicate = attempt.Events[(position - 1) / 2];
                    throw new DuplicateEventException(table, stream.Partition, duplicate.Id);
                }
                
                if (conflicting is EventEntity)
                    throw ConcurrencyConflictException.EventVersionExists(
                        table, stream.Partition, new EventVersion(conflicting.RowKey));

                var include = Array.Find(includes, x => x.Entity == conflicting);
                if (include != null)
                    throw new IncludedOperationConflictException(table, stream.Partition, include);

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

        class SetPropertiesOperation
        {
            readonly CloudTable table;
            readonly Stream stream;
            readonly StreamProperties properties;

            public SetPropertiesOperation(CloudTable table, Stream stream, StreamProperties properties)
            {
                this.table = table;
                this.stream = stream;
                this.properties = properties;
            }

            StreamEntity newStreamEntity;

            public Task ExecuteAsync()
            {
                var newStream = stream.SetProperties(properties);
                newStreamEntity = newStream.Entity();

                var merge = TableOperation.Replace(newStreamEntity);
                return table.ExecuteAsync(merge);
            }

            public Stream Result()
            {
                return From(newStreamEntity);
            }
        }

        class OpenStreamOperation
        {
            readonly CloudTable table;
            readonly string partition;

            public OpenStreamOperation(CloudTable table, string partition)
            {
                this.table = table;
                this.partition = partition;
            }

            public async Task<StreamEntity> ExecuteAsync()
            {
                var retrieve = TableOperation.Retrieve<StreamEntity>(partition, StreamEntity.FixedRowKey);
                var entity = (await table.ExecuteAsync(retrieve).Really()).Result;
                return entity != null ? (StreamEntity) entity : null;
            }
        }

        class ReadOperation<T> where T : class, new()
        {
            readonly CloudTable table;
            readonly string partition;
            readonly int startVersion;
            readonly int sliceSize;

            public ReadOperation(CloudTable table, string partition, int startVersion, int sliceSize)
            {
                this.table = table;
                this.partition = partition;
                this.startVersion = startVersion;
                this.sliceSize = sliceSize;
            }

            public async Task<StreamSlice<T>> ExecuteAsync()
            {
                var result = await QueryAsync();

                var stream = BuildStream(result.Stream);
                var events = BuildEvents(result.Events);
                
                return new StreamSlice<T>(stream, events, startVersion, sliceSize);
            }

            async Task<QueryResult> QueryAsync()
            {
                var entities = await ExecuteQuery(BuildQuery());

                var streamEntity = FindStreamEntity(entities);
                entities.Remove(streamEntity);

                return new QueryResult
                {
                    Stream = streamEntity,
                    Events = entities.ToArray()
                };
            }

            class QueryResult
            {
                public DynamicTableEntity Stream;
                public DynamicTableEntity[] Events;
            }

            TableQuery<DynamicTableEntity> BuildQuery()
            {
                var rowKeyStart = new EventKey(startVersion);
                var rowKeyEnd = new EventKey(startVersion + sliceSize - 1);

                var query = table
                    .CreateQuery<DynamicTableEntity>()
                    .Where(x =>
                           x.PartitionKey == partition
                           && (x.RowKey == StreamEntity.FixedRowKey
                               || (x.RowKey.CompareTo(rowKeyStart)  >= 0
                                   && x.RowKey.CompareTo(rowKeyEnd) <= 0)));

                return (TableQuery<DynamicTableEntity>) query;
            }

            async Task<List<DynamicTableEntity>> ExecuteQuery(TableQuery<DynamicTableEntity> query)
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

            static DynamicTableEntity FindStreamEntity(IEnumerable<DynamicTableEntity> entities)
            {
                return entities.Single(x => x.RowKey == StreamEntity.FixedRowKey);
            }

            static Stream BuildStream(DynamicTableEntity entity)
            {
                return From(StreamEntity.From(entity));
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
