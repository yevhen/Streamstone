using System;
using System.Collections.Generic;
using System.Linq;

using Streamstone;
using Streamstone.Utility;

using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class S10_Stream_directory : Scenario
    {
        public override void Run()
        {
            MultipleStreamsPerPartitionUsingStreamProperties();
            MultipleStreamsPerPartitionUsingProjection();
            SingleStreamPerPartitionUsingIndirectionLayer();
        }

        void MultipleStreamsPerPartitionUsingStreamProperties()
        {
            var properties = StreamProperties.From(new {RowType="STREAM"});
            
            Stream.Provision(VirtualPartition("11"), properties);
            Stream.Provision(VirtualPartition("22"), properties);

            // the below code will scan all rows in a single physical partition
            // also, if there more than 1000 streams (header rows), pagination need to be utilized as per regular ATS limits

            var count = Partition.Table.CreateQuery<StreamHeaderEntity>()
                                 .Where(x => x.PartitionKey == Partition.PartitionKey && 
                                             x.RowType == "STREAM")
                                 .ToList()
                                 .Count();

            Console.WriteLine(count);
        }

        void MultipleStreamsPerPartitionUsingProjection()
        {
            Stream.Write(
                new Stream(VirtualPartition("sid-33")),
                Event(Include.Insert(new StreamHeaderEntity("sid-33"))));

            Stream.Write(
                new Stream(VirtualPartition("sid-44")),
                Event(Include.Insert(new StreamHeaderEntity("sid-44"))));

            // the below code will scan only a limited range of rows in a single physical partition
            // also, if there more than 1000 streams (header rows), pagination need to be utilized as per regular ATS limits

            var count = Partition
                .RowKeyPrefixQuery<DynamicTableEntity>(StreamHeaderEntity.Prefix)
                .ToList()
                .Count();

            Console.WriteLine(count);
        }

        void SingleStreamPerPartitionUsingIndirectionLayer()
        {
            var store = new EventStore(new Partition(Table, "DIR"));

            store.Provision(VirtualPartition("vs-111"));
            store.Provision(VirtualPartition("vs-222"));

            store.Write(new Stream(new Partition(Partition.Table, "ps-333")), Event());
            store.Write(new Stream(new Partition(Partition.Table, "ps-444")), Event());

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
            {}

            public StreamHeaderEntity(string id)
            {
                RowKey = Prefix + id;
            }

            public string RowType {get; set;}
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
                this.directory.Table.CreateIfNotExists();
            }

            public Stream Provision(Partition partition)
            {
                Record(partition);
                return Stream.Provision(partition);
            }

            public StreamWriteResult Write(Stream stream, params EventData[] events)
            {
                if (stream.IsTransient)
                    Record(stream.Partition);

                return Stream.Write(stream, events);
            }

            void Record(Partition partition)
            {
                var header = new DynamicTableEntity(directory.PartitionKey, partition.ToString());
                directory.Table.Execute(TableOperation.Insert(header));
            }

            public IEnumerable<string> Streams()
            {
                // NOTE: if there more than 1000 streams (header rows) in directory,
                //            pagination need to be implemented as per regular ATS limits

                return directory.Table.CreateQuery<DynamicTableEntity>()
                                .Where(x => x.PartitionKey == directory.PartitionKey)
                                .ToArray()
                                .Select(x => x.RowKey);
            }
        }
    }
}
