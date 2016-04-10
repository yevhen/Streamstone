using Streamstone;

namespace Example.Scenarios
{
    using Microsoft.WindowsAzure.Storage;

    public class S11_Sharding_streams : Scenario
    {
        readonly CloudStorageAccount[] pool =              
        {
            CloudStorageAccount.DevelopmentStorageAccount,
            CloudStorageAccount.DevelopmentStorageAccount // pretend this is some other account
        };

        public override void Run()
        {
            var partition1 = Resolve("shard-test-1");
            var partition2 = Resolve("shard-test-2");

            Stream.Provision(partition1);
            Stream.Provision(partition2);
        }

        Partition Resolve(string stream)
        {
            var account = pool[Shard.Resolve(stream, pool.Length)];
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(Table.Name);
            return new Partition(table, $"{Partition.Key}_{stream}");
        }
    }
}
