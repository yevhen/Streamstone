using System;
using System.Linq;
using System.Threading.Tasks;

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
            return ProvisionAsync(table, new Stream(partition, properties));
        }

        public static Task<Stream> ProvisionAsync(CloudTable table, Stream stream)
        {
            return new ProvisionOperation(table, stream).ExecuteAsync();
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, Stream stream, Event[] events)
        {
            return WriteAsync(table, stream, events, NoIncludes);
        }

        public static Task<StreamWriteResult> WriteAsync(CloudTable table, Stream stream, Event[] events, Include[] includes)
        {
            return new WriteOperation(table, stream, events, includes).ExecuteAsync();
        }

        public static Task<Stream> SetPropertiesAsync(CloudTable table, Stream stream, StreamProperties properties)
        {
            return new SetPropertiesOperation(table, stream, properties).ExecuteAsync();
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

        public static async Task<bool> ExistsAsync(CloudTable table, string partition)
        {
            return (await TryOpenAsync(table, partition).Really()).Success;
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