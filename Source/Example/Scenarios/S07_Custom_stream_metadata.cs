using System;
using System.Collections.Generic;
using System.Linq;

using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class S07_Custom_stream_metadata : Scenario
    {
        public override void Run()
        {
            SpecifyingForExistingStream();
            SpecifyingDuringWritingToNewStream();
            UpdatingForExistingStream();
        }

        void SpecifyingForExistingStream()
        {
            var partition = new Partition(Table, Id + ".a");

            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };
            
            var stream = new Stream(partition, properties);
            Stream.Provision(stream);

            Console.WriteLine("Stream metadata specified during provisioning in partition '{0}'", partition);

            stream = Stream.Open(partition);
            Print(stream.Properties);
        }

        void SpecifyingDuringWritingToNewStream()
        {
            var partition = new Partition(Table, Id + ".b");

            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };

            var stream = new Stream(partition, properties);
            Stream.Write(stream, new[]{new Event("42")});

            Console.WriteLine("Stream metadata specified during writing to new stream in partition '{0}'", partition);

            stream = Stream.Open(partition);
            Print(stream.Properties);
        }

        void UpdatingForExistingStream()
        {
            var partition = new Partition(Table, Id + ".c");

            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };

            var stream = new Stream(partition, properties);
            Stream.Provision(stream);

            Console.WriteLine("Stream metadata specified for stream in partition '{0}'", partition);

            stream = Stream.Open(partition);
            Print(stream.Properties);

            properties["Active"] = new EntityProperty(false);
            Stream.SetProperties(stream, properties);

            Console.WriteLine("Updated stream metadata in partition '{0}'", partition);

            stream = Stream.Open(partition);
            Print(stream.Properties);
        }

        static void Print(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            foreach (var property in properties)
                Console.WriteLine("\t{0}={1}", property.Key, property.Value.PropertyAsObject);
        }
    }
}
