using System;
using System.Linq;

using Streamstone;
using System.Threading.Tasks;

namespace Example.Scenarios
{
    public class S03_Try_open_stream_for_writing : Scenario
    {
        public override async Task Run()
        {
            await TryOpenNonExistentStream();            
            await TryOpenExistentStream();            
        }

        async Task TryOpenNonExistentStream()
        {
            var existent = await Stream.TryOpenAsync(Partition);

            Console.WriteLine("Trying to open non-existent stream. Found: {0}, Stream: {1}", 
                              existent.Found, existent.Stream == null ? "<null>" : "?!?");
        }

        async Task TryOpenExistentStream()
        {
            await Stream.ProvisionAsync(Partition);

            var existent = await Stream.TryOpenAsync(Partition);

            Console.WriteLine("Trying to open existent stream. Found: {0}, Stream: {1}\r\nEtag - {2}, Version - {3}",
                               existent.Found, existent.Stream, existent.Stream.ETag, existent.Stream.Version);
        }
    }
}
