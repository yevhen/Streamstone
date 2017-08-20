using System;
using System.Threading.Tasks;

using Streamstone;

namespace Example.Scenarios
{
    public class S01_Provision_new_stream : Scenario
    {
        public override async Task RunAsync()
        {
            var stream = await Stream.ProvisionAsync(Partition);

            Console.WriteLine("Provisioned new empty stream in partition '{0}'", stream.Partition);
            Console.WriteLine("Etag: {0}",       stream.ETag);
            Console.WriteLine("Version: {0}",    stream.Version);

            var exists = await Stream.ExistsAsync(Partition);
            Console.WriteLine("Checking stream exists in a storage: {0}", exists);
        }
    }
}
