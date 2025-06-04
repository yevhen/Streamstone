# Sharding Streams

This scenario demonstrates how to shard streams in Streamstone.


```csharp title="S11_Sharding_streams.cs"
using System.Threading.Tasks;

using Azure.Data.Tables;

using Streamstone;

namespace Example.Scenarios
{
    public class S11_Sharding_streams : Scenario
    {
        const string DevelopmentConnectionString = "UseDevelopmentStorage=true";

        readonly TableServiceClient[] pool =              
        {
            new TableServiceClient(DevelopmentConnectionString),
            new TableServiceClient(DevelopmentConnectionString) // pretend this is some other account
        };

        public override async Task RunAsync()
        {
            var partition1 = Resolve("shard-test-1");
            var partition2 = Resolve("shard-test-2");

            await Stream.ProvisionAsync(partition1);
            await Stream.ProvisionAsync(partition2);
        }

        Partition Resolve(string stream)
        {
            var client = pool[Shard.Resolve(stream, pool.Length)];
            var table = client.GetTableClient(Table.Name);
            return new Partition(table, $"{Partition.Key}_{stream}");
        }
    }
}
``` 