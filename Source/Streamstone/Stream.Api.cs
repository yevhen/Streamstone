using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public static Stream Provision(Partition partition)
        {
            return Provision(new Stream(partition));
        }

        public static Stream Provision(Stream stream)
        {
            return new ProvisionOperation(stream.Partition.Table, stream).Execute();
        }

        public static Task<Stream> ProvisionAsync(Partition partition)
        {
            return ProvisionAsync(new Stream(partition));
        }

        public static Task<Stream> ProvisionAsync(Stream stream)
        {
            return new ProvisionOperation(stream.Partition.Table, stream).ExecuteAsync();
        }

        static readonly Include[] NoIncludes = new Include[0];

        public static StreamWriteResult Write(Stream stream, Event[] events)
        {
            return Write(stream, events, NoIncludes);
        }

        public static StreamWriteResult Write(Stream stream, Event[] events, Include[] includes)
        {
            return new WriteOperation(stream.Partition.Table, stream, events, includes).Execute();
        }

        public static Task<StreamWriteResult> WriteAsync(Stream stream, Event[] events)
        {
            return WriteAsync(stream, events, NoIncludes);
        }

        public static Task<StreamWriteResult> WriteAsync(Stream stream, Event[] events, Include[] includes)
        {
            return new WriteOperation(stream.Partition.Table, stream, events, includes).ExecuteAsync();
        }

        public static Stream SetProperties(Stream stream, IDictionary<string, EntityProperty> properties)
        {
            return new SetPropertiesOperation(stream.Partition.Table, stream, StreamProperties.From(properties)).Execute();
        }

        public static Task<Stream> SetPropertiesAsync(Stream stream, IDictionary<string, EntityProperty> properties)
        {
            return new SetPropertiesOperation(stream.Partition.Table, stream, StreamProperties.From(properties)).ExecuteAsync();
        }

        public static Stream Open(Partition partition)
        {
            var result = TryOpen(partition);

            if (result.Found)
                return result.Stream;

            throw new StreamNotFoundException(partition.Table, partition);
        }

        public static StreamOpenResult TryOpen(Partition partition)
        {
            return new OpenStreamOperation(partition.Table, partition).Execute();
        }

        public static async Task<Stream> OpenAsync(Partition partition)
        {
            var result = await TryOpenAsync(partition).Really();

            if (result.Found)
                return result.Stream;

            throw new StreamNotFoundException(partition.Table, partition);
        }

        public static Task<StreamOpenResult> TryOpenAsync(Partition partition)
        {
            return new OpenStreamOperation(partition.Table, partition).ExecuteAsync();
        }

        public static bool Exists(Partition partition)
        {
            return TryOpen(partition).Found;
        }

        public static async Task<bool> ExistsAsync(Partition partition)
        {
            return (await TryOpenAsync(partition).Really()).Found;
        }

        const int DefaultSliceSize = 1000;

        public static StreamSlice<T> Read<T>(
            Partition partition, 
            int startVersion = 1, 
            int sliceSize = DefaultSliceSize) 
            where T : class, new()
        {
            return new ReadOperation<T>(partition.Table, partition, startVersion, sliceSize).Execute();
        }
        
        public static Task<StreamSlice<T>> ReadAsync<T>(
            Partition partition, 
            int startVersion = 1, 
            int sliceSize = DefaultSliceSize) 
            where T : class, new()
        {
            return new ReadOperation<T>(partition.Table, partition, startVersion, sliceSize).ExecuteAsync();
        }
    }
}