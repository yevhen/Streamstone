using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Streamstone;
using Streamstone.Utility;

using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class S10_Stream_directory : Scenario
    {
        public override async Task RunAsync()
        {
            await MultipleStreamsPerPartitionUsingStreamProperties();
            await MultipleStreamsPerPartitionUsingProjection();
            await SingleStreamPerPartitionUsingIndirectionLayer();
        }

        /// <summary>
        /// This the simplest approach. You just need to create an additional stream metadata column and then you can simply query on it.
        /// 
        /// It's also the slowest approach of all, since all rows in a partition need to scanned. Still, it should 
        /// perform quite well for majority of apps as there won't be too many rows in a single physical partition.
        /// </summary>
        async Task MultipleStreamsPerPartitionUsingStreamProperties()
        {
            var properties = StreamProperties.From(new { RowType = "STREAM" });

            await Stream.ProvisionAsync(VirtualPartition("11"), properties);
            await Stream.ProvisionAsync(VirtualPartition("22"), properties);

            // the below code will scan all rows in a single physical partition
            // also, if there more than 1000 streams (header rows), pagination need to be utilized as per regular ATS limits

            var filter = TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, Partition.PartitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(nameof(StreamHeaderEntity.RowType), QueryComparisons.Equal, "STREAM")
                );

            var count = Partition.Table
                .ExecuteQuery<StreamHeaderEntity>(filter)
                .Count();

            Console.WriteLine(count);
        }

        /// <summary>
        /// This approach is a bit more complex, since you will need to track the start of lifecycle of the stream and include projection of its header. 
        /// The projection row will be simply a reverse rowkey of stream header entity, so that you can query a range of rows using prefix query.
        /// 
        /// This is the most performant way to query all streams(headers) in a single physical partition.  There is no any other approach which is more 
        /// performant than this one.  The only downside, it could only be used along with Stream.WriteAsync since at the moment Streamstone doesn't support 
        /// inclusion of additional entities when provisioning streams.
        /// </summary>
        async Task MultipleStreamsPerPartitionUsingProjection()
        {
            await Stream.WriteAsync(
                new Stream(VirtualPartition("sid-33")),
                Event(Include.Insert(new StreamHeaderEntity("sid-33"))));

            await Stream.WriteAsync(
                new Stream(VirtualPartition("sid-44")),
                Event(Include.Insert(new StreamHeaderEntity("sid-44"))));

            // the below code will scan only a limited range of rows in a single physical partition
            // also, if there more than 1000 streams (header rows), pagination need to be utilized as per regular ATS limits

            var count = Partition
                .RowKeyPrefixQuery<DynamicTableEntity>(StreamHeaderEntity.Prefix)
                .ToList()
                .Count;

            Console.WriteLine(count);
        }

        /// <summary>
        /// For this way you may simply create a facade through which all stream operations will go. Behind the curtain, you will record (track) all 
        /// created streams in some dedicated partition, so that you can simply query single partition to get information about all streams in your 
        /// system. Basically, it's a just an implementation of multi-tenancy.
        ///
        /// This last approach is little bit more involved but with stream-per-partition it is the only possible approach.  There will be some additional 
        /// complexity related to maintaining consistency between directory partition and actual stream partition, since there is no cross-partition 
        /// transactions in WATS.  But that should be a really rare case (failure to write stream after recording it in directory) and can be resolved 
        /// with manual intervention.
        /// </summary>
        async Task SingleStreamPerPartitionUsingIndirectionLayer()
        {
            var store = new EventStore(new Partition(Table, "DIR"));

            await store.ProvisionAsync(VirtualPartition("vs-111"));
            await store.ProvisionAsync(VirtualPartition("vs-222"));

            await store.WriteAsync(new Stream(new Partition(Partition.Table, "ps-333")), Event());
            await store.WriteAsync(new Stream(new Partition(Partition.Table, "ps-444")), Event());

            var count = store.Streams().Count();
            Console.WriteLine(count);
        }

        Partition VirtualPartition(string stream)
        {
            return new Partition(Partition.Table, Partition.PartitionKey + "|" + stream);
        }

        class StreamHeaderEntity : TableEntity
        {
            public const string Prefix = "STREAM|";

            public StreamHeaderEntity()
            { }

            public StreamHeaderEntity(string id)
            {
                RowKey = Prefix + id;
            }

            public string RowType { get; set; }
        }

        static EventData Event(params Include[] includes)
        {
            return new EventData(EventId.None, EventIncludes.From(includes));
        }

        class EventStore
        {
            readonly Partition directory;

            public EventStore(Partition directory)
            {
                this.directory = directory;
                this.directory.Table.CreateIfNotExistsAsync().Wait();
            }

            public async Task<Stream> ProvisionAsync(Partition partition)
            {
                await Record(partition);
                return await Stream.ProvisionAsync(partition);
            }

            public async Task<StreamWriteResult> WriteAsync(Stream stream, params EventData[] events)
            {
                if (stream.IsTransient)
                    await Record(stream.Partition);

                return await Stream.WriteAsync(stream, events);
            }

            async Task Record(Partition partition)
            {
                var header = new DynamicTableEntity(directory.PartitionKey, partition.ToString());
                await directory.Table.ExecuteAsync(TableOperation.Insert(header));
            }

            public IEnumerable<string> Streams()
            {
                // NOTE: if there more than 1000 streams (header rows) in directory,
                //       pagination need to be implemented as per regular ATS limits
                var filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, directory.PartitionKey);
                return directory.Table.ExecuteQuery<DynamicTableEntity>(filter)
                                .Select(x => x.RowKey);
            }
        }
    }
}
