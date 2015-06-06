using System;
using System.Linq;
using System.Threading.Tasks;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public static Stream Provision(Partition partition)
        {
            return Provision(new Stream(partition));
        }

        public static Stream Provision(Partition partition, StreamProperties properties)
        {
            return Provision(new Stream(partition, properties));
        }

        static Stream Provision(Stream stream)
        {
            Requires.NotNull(stream, "stream");
            return new ProvisionOperation(stream).Execute();
        }

        public static Task<Stream> ProvisionAsync(Partition partition)
        {
            return ProvisionAsync(new Stream(partition));
        }

        public static Task<Stream> ProvisionAsync(Partition partition, StreamProperties properties)
        {
            return ProvisionAsync(new Stream(partition, properties));
        }

        static Task<Stream> ProvisionAsync(Stream stream)
        {
            Requires.NotNull(stream, "stream");
            return new ProvisionOperation(stream).ExecuteAsync();
        }

        public static StreamWriteResult Write(Stream stream, EventData[] events, bool ded = true)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(events, "events");

            if (events.Length == 0)
                throw new ArgumentOutOfRangeException("events", "Events have 0 items");

            return new WriteOperation(stream, events, ded).Execute();
        }

        public static Task<StreamWriteResult> WriteAsync(Stream stream, EventData[] events, bool ded = true)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(events, "events");

            if (events.Length == 0)
                throw new ArgumentOutOfRangeException("events", "Events have 0 items");

            return new WriteOperation(stream, events, ded).ExecuteAsync();
        }

        public static Stream SetProperties(Stream stream, StreamProperties properties)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            if (stream.IsTransient)
                throw new ArgumentException("Can't set properties on transient stream", "stream");

            return new SetPropertiesOperation(stream, properties).Execute();
        }

        public static Task<Stream> SetPropertiesAsync(Stream stream, StreamProperties properties)
        {
            Requires.NotNull(stream, "stream");
            Requires.NotNull(properties, "properties");

            if (stream.IsTransient)
                throw new ArgumentException("Can't set properties on transient stream", "stream");

            return new SetPropertiesOperation(stream, properties).ExecuteAsync();
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
            Requires.NotNull(partition, "partition");

            return new OpenStreamOperation(partition).Execute();
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
            Requires.NotNull(partition, "partition");

            return new OpenStreamOperation(partition).ExecuteAsync();
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
            Requires.NotNull(partition, "partition");
            Requires.GreaterThanOrEqualToOne(startVersion, "startVersion");
            Requires.GreaterThanOrEqualToOne(sliceSize, "sliceSize");
            
            return new ReadOperation<T>(partition, startVersion, sliceSize).Execute();
        }
        
        public static Task<StreamSlice<T>> ReadAsync<T>(
            Partition partition, 
            int startVersion = 1, 
            int sliceSize = DefaultSliceSize) 
            where T : class, new()
        {
            Requires.NotNull(partition, "partition");
            Requires.GreaterThanOrEqualToOne(startVersion, "startVersion");
            Requires.GreaterThanOrEqualToOne(sliceSize, "sliceSize");

            return new ReadOperation<T>(partition, startVersion, sliceSize).ExecuteAsync();
        }
    }
}