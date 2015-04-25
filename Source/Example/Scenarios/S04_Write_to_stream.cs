using System;
using System.Linq;

using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

using Streamstone;
using System.Diagnostics;

namespace Example.Scenarios
{
    public class S04_Write_to_stream : Scenario
    {
        public override void Run()
        {
            WriteToExistingOrCreateNewStream();
            WriteSequentiallyToExistingStream();
            WriteMultipleStreamsInParallel();
        }

        void WriteToExistingOrCreateNewStream()
        {
            var existent = Stream.TryOpen(Table, Partition);

            var stream = existent.Found 
                ? existent.Stream 
                : new Stream(Partition);

            Console.WriteLine("Writing to new stream in partition '{0}'", stream.Partition);

            var result = Stream.Write(Table, stream, new[]
            {
                Event(new InventoryItemCreated(Partition, "iPhone6")),
                Event(new InventoryItemCheckedIn(Partition, 100)),
            });

            Console.WriteLine("Succesfully written to new stream.\r\nEtag: {0}, Version: {1}", 
                              result.Stream.ETag, result.Stream.Version);
        }

        void WriteSequentiallyToExistingStream()
        {
            var stream = Stream.Open(Table, Partition);

            Console.WriteLine("Writing sequentially to existing stream in partition '{0}'", stream.Partition);
            Console.WriteLine("Etag: {0}, Version: {1}", stream.ETag, stream.Version);

            for (int i = 1; i <= 10; i++)
            {
                var result = Stream.Write(Table, stream, new[]
                {
                    Event(new InventoryItemCheckedIn(Partition, i*100)),
                });

                Console.WriteLine("Succesfully written event '{0}' under version '{1}'",
                                   result.Events[0].Id, result.Events[0].Version);

                Console.WriteLine("Etag: {0}, Version: {1}",
                                   result.Stream.ETag, result.Stream.Version);

                stream = result.Stream;
            }
        }

        void WriteMultipleStreamsInParallel()
        {
            Enumerable.Range(1, 50).AsParallel()
                .ForAll(streamIndex =>
                {
                    var stream = new Stream(string.Concat(Partition, "-", streamIndex));
                    Console.WriteLine("Writing to new stream in partition '{0}'", stream.Partition);
                    var stopwatch = Stopwatch.StartNew();

                    for (int i = 1; i <= 3; i++)
                    {
                        var events = Enumerable.Range(1, 10)
                            .Select(_ => Event(new InventoryItemCheckedIn(Partition, i * 1000 + streamIndex)))
                            .ToArray();

                        var result = Stream.Write(Table, stream, events);

                        stream = result.Stream;
                    }

                    stopwatch.Stop();
                    Console.WriteLine("Finished writing 300 events to new stream in partition '{0}' in {1}ms", stream.Partition, stopwatch.ElapsedMilliseconds);
                });
        }

        static Event Event(object e)
        {
            var id = Guid.NewGuid();

            var data = new
            {
                Id = id,                 // id that you specify for Event ctor is used only for idempotency
                Type = e.GetType().Name, // you can include any number of custom properties along with event
                Data = JSON(e),          // you're free to choose any name you like for data property
                Bin = BSON(e)            // and any storage format: binary, string, whatever (any EdmType)
            };

            return new Event(id.ToString("D"), data.Props());
        }

        static string JSON(object data)
        {
            return JsonConvert.SerializeObject(data);
        }

        static byte[] BSON(object data)
        {
            var stream = new System.IO.MemoryStream();
            
            using (var writer = new BsonWriter(stream))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, data);
            }

            return stream.ToArray();
        }
    }
}
