using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public static Stream Provision(CloudTable table, string partition)
        {
            return Provision(table, new Stream(partition));
        }

        public static Stream Provision(CloudTable table, Stream stream)
        {
            return new ProvisionOperation(table, stream).Execute();
        }

        public static Task<Stream> ProvisionAsync(CloudTable table, string partition)
        {
            return ProvisionAsync(table, new Stream(partition));
        }

        public static Task<Stream> ProvisionAsync(CloudTable table, Stream stream)
        {
            return new ProvisionOperation(table, stream).ExecuteAsync();
        }

        static readonly Include[] NoIncludes = new Include[0];

        public static StreamWriteResult Write(CloudTable table, Stream stream, Event[] events)
        {
            return Write(table, stream, events, NoIncludes);
        }

        public static StreamWriteResult Write(CloudTable table, Stream stream, Event[] events, Include[] includes)
        {
            return new WriteOperation(table, stream, events, includes).Execute();
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, Stream stream, Event[] events)
        {
            return WriteAsync(table, stream, events, NoIncludes);
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, Stream stream, Event[] events, Include[] includes)
        {
            return new WriteOperation(table, stream, events, includes).ExecuteAsync();
        }

        public static Stream SetProperties(CloudTable table, Stream stream, ITableEntity properties)
        {
            return SetProperties(table, stream, StreamProperties.From(properties));
        }

        public static Stream SetProperties(CloudTable table, Stream stream, object properties)
        {
            return SetProperties(table, stream, StreamProperties.From(properties));
        }

        public static Stream SetProperties(CloudTable table, Stream stream, IDictionary<string, EntityProperty> properties)
        {
            return SetProperties(table, stream, StreamProperties.From(properties));
        }

        static Stream SetProperties(CloudTable table, Stream stream, StreamProperties properties)
        {
            return new SetPropertiesOperation(table, stream, properties).Execute();
        }

        public static Task<Stream> SetPropertiesAsync(CloudTable table, Stream stream, ITableEntity properties)
        {
            return SetPropertiesAsync(table, stream, StreamProperties.From(properties));
        }

        public static Task<Stream> SetPropertiesAsync(CloudTable table, Stream stream, object properties)
        {
            return SetPropertiesAsync(table, stream, StreamProperties.From(properties));
        }

        public static Task<Stream> SetPropertiesAsync(CloudTable table, Stream stream, IDictionary<string, EntityProperty> properties)
        {
            return SetPropertiesAsync(table, stream, StreamProperties.From(properties));
        }

        static Task<Stream> SetPropertiesAsync(CloudTable table, Stream stream, StreamProperties properties)
        {
            return new SetPropertiesOperation(table, stream, properties).ExecuteAsync();
        }

        public static Stream Open(CloudTable table, string partition)
        {
            var result = TryOpen(table, partition);

            if (result.Success)
                return result.Stream;

            throw new StreamNotFoundException(table, partition);
        }

        public static StreamOpenResult TryOpen(CloudTable table, string partition)
        {
            return new OpenStreamOperation(table, partition).Execute();
        }

        public static async Task<Stream> OpenAsync(CloudTable table, string partition)
        {
            var result = await TryOpenAsync(table, partition).Really();

            if (result.Success)
                return result.Stream;

            throw new StreamNotFoundException(table, partition);
        }

        public static Task<StreamOpenResult> TryOpenAsync(CloudTable table, string partition)
        {
            return new OpenStreamOperation(table, partition).ExecuteAsync();
        }

        public static bool Exists(CloudTable table, string partition)
        {
            return TryOpen(table, partition).Success;
        }
        
        public static async Task<bool> ExistsAsync(CloudTable table, string partition)
        {
            return (await TryOpenAsync(table, partition).Really()).Success;
        }

        const int DefaultSliceSize = 500;

        public static StreamSlice<T> Read<T>(
            CloudTable table, 
            string partition, 
            int startVersion = 1, 
            int sliceSize = DefaultSliceSize) 
            where T : class, new()
        {
            return new ReadOperation<T>(table, partition, startVersion, sliceSize).Execute();
        }
        
        public static Task<StreamSlice<T>> ReadAsync<T>(
            CloudTable table, 
            string partition, 
            int startVersion = 1, 
            int sliceSize = DefaultSliceSize) 
            where T : class, new()
        {
            return new ReadOperation<T>(table, partition, startVersion, sliceSize).ExecuteAsync();
        }
    }
}