using System;
using System.Linq;

using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

using Streamstone;

namespace Example.Scenarios
{
    public class S04_Write_to_stream : Scenario
    {
        public override void Run()
        {
            WriteToExistingOrCreateNewStream();
            WriteSequentiallyToExistingStream();
        }

        void WriteToExistingOrCreateNewStream()
        {
            var existent = Stream.TryOpen(Partition);

            var stream = existent.Found 
                ? existent.Stream 
                : new Stream(Partition);

            Console.WriteLine("Writing to new stream in partition '{0}'", stream.Partition);

            var result = Stream.Write(stream, new[]
            {
                Event(new InventoryItemCreated(Id, "iPhone6")),
                Event(new InventoryItemCheckedIn(Id, 100)),
            });

            Console.WriteLine("Succesfully written to new stream.\r\nEtag: {0}, Version: {1}", 
                              result.Stream.ETag, result.Stream.Version);
        }

        void WriteSequentiallyToExistingStream()
        {
            var stream = Stream.Open(Partition);

            Console.WriteLine("Writing sequentially to existing stream in partition '{0}'", stream.Partition);
            Console.WriteLine("Etag: {0}, Version: {1}", stream.ETag, stream.Version);

            for (int i = 1; i <= 10; i++)
            {
                var result = Stream.Write(stream, new[]
                {
                    Event(new InventoryItemCheckedIn(Id, i*100)),
                });

                Console.WriteLine("Succesfully written event '{0}' under version '{1}'", 
                                   result.Events[0].Id, result.Events[0].Version);

                Console.WriteLine("Etag: {0}, Version: {1}",
                                   result.Stream.ETag, result.Stream.Version);

                stream = result.Stream;
            }
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

            return new EventData(id.ToString("D"), EventProperties.From(properties));
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
