using System;
using System.Linq;

using Streamstone;

namespace Example.Scenarios
{
    public class S01_Provision_new_stream : Scenario
    {
        public override void Run()
        {
            var stream = Stream.Provision(Partition);

            Console.WriteLine("Provisioned new empty stream in partition '{0}'", stream.Partition);
            Console.WriteLine("Etag: {0}",       stream.ETag);
            Console.WriteLine("Version: {0}",    stream.Version);

            var exists = Stream.Exists(Partition);
            Console.WriteLine("Checking stream exists in a storage: {0}", exists);
        }
    }
}
