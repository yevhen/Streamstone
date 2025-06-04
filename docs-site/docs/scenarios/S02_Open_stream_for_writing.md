# Opening a Stream

This scenario demonstrates how to open a stream in Streamstone.

```csharp title="S02_Open_stream_for_writing.cs"
using System;
using System.Threading.Tasks;

using Streamstone;

namespace Example.Scenarios
{
    public class S02_Open_stream_for_writing : Scenario
    {
        public override async Task RunAsync()
        {
            await OpenNonExistingStream();
            await OpenExistingStream();
        }

        async Task OpenNonExistingStream()
        {
            try
            {
                await Stream.OpenAsync(Partition);
            }
            catch (StreamNotFoundException)
            {
                Console.WriteLine("Opening non-existing stream will throw StreamNotFoundException");
            }
        }

        async Task OpenExistingStream()
        {
            await Stream.ProvisionAsync(Partition);

            var stream = await Stream.OpenAsync(Partition);

            Console.WriteLine("Opened existing (empty) stream in partition '{0}'", stream.Partition);
            Console.WriteLine("Etag: {0}", stream.ETag);
            Console.WriteLine("Version: {0}", stream.Version);
        }
    }
} 