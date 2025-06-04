# Optimistic Concurrency

This scenario demonstrates how to handle optimistic concurrency conflicts in Streamstone.


```csharp title="S08_Concurrency_conflicts.cs"
using System;
using System.Threading.Tasks;

using Streamstone;

namespace Example.Scenarios
{
    public class S08_Concurrency_conflicts : Scenario
    {
        public override async Task RunAsync()
        {
            await SimultaneousProvisioning();
            await SimultaneousWriting();
            await SimultaneousSettingOfStreamMetadata();
            await SequentiallyWritingToStreamIgnoringReturnedStreamHeader();
        }

        async Task SimultaneousProvisioning()
        {
            await Stream.ProvisionAsync(Partition);

            try
            {
                await Stream.ProvisionAsync(Partition);
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously provisioning stream in a same partition will lead to ConcurrencyConflictException");
            }
        }

        async Task SimultaneousWriting()
        {
            var a = await Stream.OpenAsync(Partition);
            var b = await Stream.OpenAsync(Partition);

            await Stream.WriteAsync(a, new EventData(EventId.From("123")));
            
            try
            {
                await Stream.WriteAsync(b, new EventData(EventId.From("456")));
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously writing to the same version of stream will lead to ConcurrencyConflictException");
            }
        }

        async Task SimultaneousSettingOfStreamMetadata()
        {
            var a = await Stream.OpenAsync(Partition);
            var b = await Stream.OpenAsync(Partition);

            await Stream.SetPropertiesAsync(a, StreamProperties.From(new {A = 42}));

            try
            {
                await Stream.SetPropertiesAsync(b, StreamProperties.From(new {A = 56}));
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously setting metadata using the same version of stream will lead to ConcurrencyConflictException");
            }
        }

        async Task SequentiallyWritingToStreamIgnoringReturnedStreamHeader()
        {
            var stream = await Stream.OpenAsync(Partition);

            var result = await Stream.WriteAsync(stream, new EventData(EventId.From("AAA")));
            
            // a new stream header is returned after each write, it contains new Etag
            // and it should be used for subsequent operations
            // stream = result.Stream; 
            
            try
            {
                await Stream.WriteAsync(stream, new EventData(EventId.From("BBB")));
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Ignoring new stream (header) returned after each Write() operation will lead to ConcurrencyConflictException on subsequent write operation");
            }
        }
    }
}
``` 