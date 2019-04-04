using System;
using System.Linq;

using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

using Streamstone;
using System.Diagnostics;

namespace Example.Scenarios
{
    using System.Threading.Tasks;

    public class S04_Write_to_stream : Scenario
    {
        public override async Task RunAsync()
        {
            await WriteToExistingOrCreateNewStream();
            await WriteSequentiallyToExistingStream();
            await WriteMultipleStreamsInParallel();
        }

        async Task WriteToExistingOrCreateNewStream()
        {
            var existent = await Stream.TryOpenAsync(Partition);

            var stream = existent.Found 
                ? existent.Stream 
                : new Stream(Partition);

            Console.WriteLine("Writing to new stream in partition '{0}'", stream.Partition);

            var result = await Stream.WriteAsync(stream,
                Event(new InventoryItemCreated(Id, "iPhone6")),
                Event(new InventoryItemCheckedIn(Id, 100)));

            Console.WriteLine("Succesfully written to new stream.\r\nEtag: {0}, Version: {1}", 
                              result.Stream.ETag, result.Stream.Version);
        }

        async Task WriteSequentiallyToExistingStream()
        {
            var stream = await Stream.OpenAsync(Partition);

            Console.WriteLine("Writing sequentially to existing stream in partition '{0}'", stream.Partition);
            Console.WriteLine("Etag: {0}, Version: {1}", stream.ETag, stream.Version);

            for (var i = 1; i <= 10; i++)
            {
                var result = await Stream.WriteAsync(stream, 
                    Event(new InventoryItemCheckedIn(Id, i*100)));

                Console.WriteLine("Succesfully written event '{0}' under version '{1}'",
                                   result.Events[0].Id, result.Events[0].Version);

                Console.WriteLine("Etag: {0}, Version: {1}",
                                   result.Stream.ETag, result.Stream.Version);

                stream = result.Stream;
            }
        }

        async Task WriteMultipleStreamsInParallel()
        {
            const int streamsToWrite = 5;

            await Task.WhenAll(Enumerable.Range(1, streamsToWrite).Select(async streamIndex =>
            {
                var partition = new Partition(Partition.Table, $"WriteMultipleStreamsInParallel-{streamIndex}");

                var existent = await Stream.TryOpenAsync(partition);

                var stream = existent.Found
                    ? existent.Stream
                    : new Stream(partition);

                Console.WriteLine("Writing to new stream in partition '{0}'", partition);
                var stopwatch = Stopwatch.StartNew();

                for (var i = 1; i <= 5; i++)
                {
                    var events = Enumerable.Range(1, 10)
                        .Select(_ => Event(new InventoryItemCheckedIn(partition.Key, i * 1000 + streamIndex)))
                        .ToArray();

                    var result = await Stream.WriteAsync(stream, events);
                    stream = result.Stream;
                }

                stopwatch.Stop();
                Console.WriteLine("Finished writing 300 events to new stream in partition '{0}' in {1}ms", stream.Partition, stopwatch.ElapsedMilliseconds);
            }));
        }

        static EventData Event(object e)
        {
            var id = Guid.NewGuid();

            var properties = new
            {
                Id = id,                 // id that you specify for Event ctor is used only for duplicate event detection
                Type = e.GetType().Name, // you can include any number of custom properties along with event
                Data = JSON(e),          // you're free to choose any name you like for data property
                Bin = BSON(e)            // and any storage format: binary, string, whatever (any EdmType)
            };

            return new EventData(EventId.From(id), EventProperties.From(properties));
        }

        static string JSON(object data)
        {
            return JsonConvert.SerializeObject(data);
        }

        static byte[] BSON(object data)
        {
            var stream = new System.IO.MemoryStream();
            
            using (var writer = new BsonDataWriter(stream))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, data);
            }

            return stream.ToArray();
        }
    }
}