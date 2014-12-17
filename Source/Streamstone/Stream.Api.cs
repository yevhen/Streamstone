using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        static readonly Include[] NoIncludes = new Include[0];
        const int DefaultSliceSize = 500;

        public static Task<Stream> ProvisionAsync(CloudTable table, string partition)
        {
            return ProvisionAsync(table, partition, StreamProperties.None);
        }

        public static Task<Stream> ProvisionAsync(CloudTable table, string partition, StreamProperties properties)
        {
            Requires.NotNull(table, "table");
            Requires.NotNullOrEmpty(partition, "partition");
            Requires.NotNull(properties, "properties");

            return ProvisionAsync(table, new Stream(partition, properties));
        }
        
        static async Task<Stream> ProvisionAsync(CloudTable table, Stream stream)
        {
            var operation = new ProvisionOperation(table, stream);

            try
            {
                await operation.ExecuteAsync().Really();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                    throw ConcurrencyConflictException.StreamChangedOrExists(table, stream.Partition);

                throw;
            }

            return operation.Result();
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, string partition, Event[] events)
        {
            return WriteAsync(table, partition, events, NoIncludes);
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, string partition, Event[] events, Include[] includes)
        {
            return WriteAsync(table, partition, StreamProperties.None, events, includes);
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, string partition, StreamProperties properties, Event[] events)
        {
            return WriteAsync(table, partition, properties, events, NoIncludes);
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, string partition, StreamProperties properties, Event[] events, Include[] includes)
        {
            Requires.NotNullOrEmpty(partition, "partition");
            Requires.NotNull(properties, "properties");

            return WriteAsync(table, new Stream(partition, properties), events, includes);
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, Stream stream, Event[] events)
        {
            return WriteAsync(table, stream, events, NoIncludes);
        }

        public static async Task<StreamWriteResult> WriteAsync(CloudTable table, Stream stream, Event[] events, Include[] includes)
        {
            Requires.NotNull(table,  "table"); 
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

            var operation = new WriteOperation(table, stream, events, includes);

            try
            {
                await operation.ExecuteAsync().Really();
            }
            catch (StorageException e)
            {               
                operation.Handle(e);
            }

            return operation.Result();
        }

        public static async Task<Stream> SetPropertiesAsync(CloudTable table, Stream stream, StreamProperties properties)
        {
            Requires.NotNull(table, "table");
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            var operation = new SetPropertiesOperation(table, stream, properties);

            try
            {
                await operation.ExecuteAsync().Really();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    throw ConcurrencyConflictException.StreamChanged(table, stream.Partition);

                throw;
            }

            return operation.Result();
        }

        public static async Task<Stream> OpenAsync(CloudTable table, string partition)
        {
            Requires.NotNull(table, "table");
            Requires.NotNullOrEmpty(partition, "partition");

            var operation = new OpenStreamOperation(table, partition);
            var entity = await operation.ExecuteAsync().Really();

            if (entity != null)
                return From(entity);

            throw new StreamNotFoundException(table, partition);
        }

        public static async Task<StreamOpenResult> TryOpenAsync(CloudTable table, string partition)
        {
            Requires.NotNull(table, "table");
            Requires.NotNullOrEmpty(partition, "partition");

            var operation = new OpenStreamOperation(table, partition);
            var entity = await operation.ExecuteAsync().Really();
            
            return entity != null
                    ? new StreamOpenResult(true, From(entity)) 
                    : StreamOpenResult.NotFound;
        }

        public static async Task<bool> ExistsAsync(CloudTable table, string partition)
        {
            return (await TryOpenAsync(table, partition).Really()).Success;
        }

        public static Task<StreamSlice<T>> ReadAsync<T>(CloudTable table, string partition, int startVersion = 1, int sliceSize = DefaultSliceSize)
            where T : class, new()
        {
            Requires.NotNull(table, "table");
            Requires.NotNullOrEmpty(partition, "partition");
            Requires.GreaterThanOrEqualToOne(startVersion, "startVersion");

            return new ReadOperation<T>(table, partition, startVersion, sliceSize).ExecuteAsync();
        }
    }
}