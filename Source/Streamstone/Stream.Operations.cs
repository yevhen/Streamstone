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
            readonly string partition;
            readonly StreamEntity streamEntity;

            public ProvisionOperation(CloudTable table, Stream stream)
            {
                Requires.NotNull(table, "table");
                Requires.NotNull(stream, "stream");

                if (stream.IsStored)
                    throw new ArgumentException("Can't provision already stored stream", "stream");

                this.table  = table;
                this.partition = stream.Partition;
                this.streamEntity = stream.Entity();
            }

            public Stream Execute()
            {
                try
                {
                    table.Execute(Prepare());
                }
                catch (StorageException e)
                {
                    Handle(e);
                }

                return Result();
            }

            public async Task<Stream> ExecuteAsync()
            {
                try
                {
                    await table.ExecuteAsync(Prepare()).Really();
                }
                catch (StorageException e)
                {
                    Handle(e);
                }

                return Result();
            }

            TableOperation Prepare()
            {
                return TableOperation.Insert(streamEntity);
            }

            void Handle(StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                    throw ConcurrencyConflictException.StreamChangedOrExists(table, partition);

                throw e.PreserveStackTrace();
            }

            Stream Result()
            {
                return From(streamEntity);
            }
        }

        class WriteOperation
        {
            readonly CloudTable table;
            readonly string partition;

            readonly TableBatchOperation batch = new TableBatchOperation();
            readonly List<ITableEntity> items = new List<ITableEntity>();

            readonly WriteTransaction transaction;
            
            public WriteOperation(CloudTable table, Stream stream, Event[] events, Include[] includes)
            {
                Requires.NotNull(table, "table");
                Requires.NotNull(stream, "stream");
                Requires.NotNull(events, "events");
                Requires.NotNull(events, "includes");

                if (events.Length == 0)
                    throw new ArgumentOutOfRangeException("events", "Events have 0 items");

                const int maxBatchSize = 100;
                const int entitiesPerEvent = 2;
                const int streamEntityPerBatch = 1;
                const int maxEntitiesPerBatch = (maxBatchSize / entitiesPerEvent) - streamEntityPerBatch;

                if (events.Length + includes.Length > maxEntitiesPerBatch)
                    throw new ArgumentOutOfRangeException("events",
                        "Maximum number of events per batch is " + maxEntitiesPerBatch);

                this.table       = table;
                this.partition   = stream.Partition;
                this.transaction = stream.Write(events, includes);
            }

            public StreamWriteResult Execute()
            {
                Prepare();

                try
                {
                    table.ExecuteBatch(batch);
                }
                catch (StorageException e)
                {
                    Handle(e);
                }

                return Result();
            }

            public async Task<StreamWriteResult> ExecuteAsync()
            {
                Prepare();

                try
                {
                    await table.ExecuteBatchAsync(batch).Really();
                }
                catch (StorageException e)
                {
                    Handle(e);
                }

                return Result();        
            }

            void Prepare()
            {
                WriteStream();
                WriteEvents();
                WriteIncludes();
            }

            void WriteStream()
            {
                var streamEntity = transaction.Stream.Entity();

                if (streamEntity.ETag == null)
                    batch.Insert(streamEntity);
                else
                    batch.Replace(streamEntity);

                items.Add(streamEntity);
            }

            void WriteEvents()
            {
                foreach (var e in transaction.Events)
                {
                    var eventEntity = e.EventEntity();
                    var eventIdEntity = e.IdEntity();

                    batch.Insert(eventEntity);
                    batch.Insert(eventIdEntity);

                    items.Add(eventEntity);
                    items.Add(eventIdEntity);
                }
            }

            void WriteIncludes()
            {
                foreach (var include in transaction.Includes)
                {
                    batch.Add(include.Apply(partition));
                    items.Add(include.Entity);
                }
            }

            StreamWriteResult Result()
            {
                var storedStream = From((StreamEntity)items.First());
                
                var storedEvents = transaction.Events
                    .Select(e => e.Stored())
                    .ToArray();

                return new StreamWriteResult(storedStream, storedEvents);
            }

            void Handle(StorageException exception)
            {
                if (exception.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    throw ConcurrencyConflictException.StreamChangedOrExists(table, partition);

                if (exception.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw exception.PreserveStackTrace();

                var error = exception.RequestInformation.ExtendedErrorInformation;
                if (error.ErrorCode != "EntityAlreadyExists")
                    throw UnexpectedStorageResponseException.ErrorCodeShouldBeEntityAlreadyExists(error);

                var position = ParseConflictingEntityPosition(error);
                Debug.Assert(position >= 0 && position < items.Count);

                var conflicting = items[position];
                if (conflicting is EventIdEntity)
                {
                    var duplicate = transaction.Events[(position - 1) / 2];
                    throw new DuplicateEventException(table, partition, duplicate.Source.Id);
                }
                
                if (conflicting is EventEntity)
                    throw ConcurrencyConflictException.EventVersionExists(
                        table, partition, new EventVersion(conflicting.RowKey));

                var include = Array.Find(transaction.Includes, x => x.Entity == conflicting);
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

        class SetPropertiesOperation
        {
            readonly CloudTable table;
            readonly string partition;
            readonly StreamEntity streamEntity;

            public SetPropertiesOperation(CloudTable table, Stream stream, StreamProperties properties)
            {
                Requires.NotNull(table, "table");
                Requires.NotNull(stream, "stream");
                Requires.NotNull(properties, "properties");

                if (stream.IsTransient)
                    throw new ArgumentException("Can't set properties on transient stream", "stream");

                this.table = table;
                this.partition = stream.Partition;
                this.streamEntity = stream.SetProperties(properties).Entity();
            }

            public Stream Execute()
            {
                try
                {
                    table.Execute(Prepare());
                }
                catch (StorageException e)
                {
                    Handle(e);
                }

                return Result();
            }

            public async Task<Stream> ExecuteAsync()
            {
                try
                {
                    await table.ExecuteAsync(Prepare()).Really();
                }
                catch (StorageException e)
                {
                    Handle(e);
                }

                return Result();
            }

            TableOperation Prepare()
            {
                return TableOperation.Replace(streamEntity);
            }

            void Handle(StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int) HttpStatusCode.PreconditionFailed)
                    throw ConcurrencyConflictException.StreamChanged(table, partition);

                throw e.PreserveStackTrace();
            }

            Stream Result()
            {
                return From(streamEntity);
            }
        }

        class OpenStreamOperation
        {
            readonly CloudTable table;
            readonly string partition;

            public OpenStreamOperation(CloudTable table, string partition)
            {
                Requires.NotNull(table, "table");
                Requires.NotNullOrEmpty(partition, "partition");

                this.table = table;
                this.partition = partition;
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
                return TableOperation.Retrieve<StreamEntity>(partition, StreamEntity.FixedRowKey);
            }

            static StreamOpenResult Result(TableResult result)
            {
                var entity = result.Result;

                return entity != null
                           ? new StreamOpenResult(true, From(((StreamEntity)entity)))
                           : StreamOpenResult.NotFound;
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
                Requires.NotNull(table, "table");
                Requires.NotNullOrEmpty(partition, "partition");
                Requires.GreaterThanOrEqualToOne(startVersion, "startVersion");
                Requires.GreaterThanOrEqualToOne(sliceSize, "sliceSize");

                this.table = table;
                this.partition = partition;
                this.startVersion = startVersion;
                this.sliceSize = sliceSize;
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
