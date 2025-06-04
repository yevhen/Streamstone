# Custom Stream Metadata

This scenario demonstrates how to use custom stream metadata in Streamstone.



```csharp title="S07_Custom_stream_metadata.cs"
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Data.Tables;

using Streamstone;

namespace Example.Scenarios
{
    public class S07_Custom_stream_metadata : Scenario
    {
        public override async Task RunAsync()
        {
            await SpecifyingForExistingStream();
            await SpecifyingDuringWritingToNewStream();
            await UpdatingForExistingStream();
        }

        async Task SpecifyingForExistingStream()
        {
            var partition = new Partition(Table, Id + ".a");

            var properties = new Dictionary<string, object>
            {
                {"Created", DateTimeOffset.Now},
                {"Active",  true}
            };
            
            await Stream.ProvisionAsync(partition, StreamProperties.From(properties));
            
            Console.WriteLine("Stream metadata specified during provisioning in partition '{0}'", 
                              partition);

            var stream = await Stream.OpenAsync(partition);
            Print(stream.Properties);
        }

        async Task SpecifyingDuringWritingToNewStream()
        {
            var partition = new Partition(Table, Id + ".b");

            var properties = new Dictionary<string, object>
            {
                {"Created", DateTimeOffset.Now},
                {"Active",  true}
            };

            var stream = new Stream(partition, StreamProperties.From(properties));
            await Stream.WriteAsync(stream, new EventData());

            Console.WriteLine("Stream metadata specified during writing to new stream in partition '{0}'", 
                              partition);

            stream = await Stream.OpenAsync(partition);
            Print(stream.Properties);
        }

        async Task UpdatingForExistingStream()
        {
            var partition = new Partition(Table, Id + ".c");

            var properties = new Dictionary<string, object>
            {
                {"Created", DateTimeOffset.Now},
                {"Active",  true}
            };

            await Stream.ProvisionAsync(partition, StreamProperties.From(properties));

            Console.WriteLine("Stream metadata specified for stream in partition '{0}'", 
                              partition);

            var stream = await Stream.OpenAsync(partition);
            Print(stream.Properties);

            properties["Active"] = false;
            await Stream.SetPropertiesAsync(stream, StreamProperties.From(properties));

            Console.WriteLine("Updated stream metadata in partition '{0}'", partition);

            stream = await Stream.OpenAsync(partition);
            Print(stream.Properties);
        }

        static void Print(IEnumerable<KeyValuePair<string, object>> properties)
        {
            foreach (var property in properties)
                Console.WriteLine("\t{0}={1}", property.Key, property.Value);
        }
    }
}
``` 