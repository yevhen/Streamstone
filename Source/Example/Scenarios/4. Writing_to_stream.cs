using System;
using System.Collections.Generic;
using System.Linq;

using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class Writing_to_stream : Scenario
    {
        public override void Run()
        {
            WriteToExistingOrCreateNewStream();
            WriteSequentiallyToExistingStream();
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
                Payload(new InventoryItemCreated("INV-004", "iPhone6")),
                Payload(new InventoryItemCheckedIn("INV-004", 100)),
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
                    Payload(new InventoryItemCheckedIn("INV-004", i*100)),
                });

                Console.WriteLine("Succesfully written event '{0}' under version '{1}'", 
                                   result.Events[0].Id, result.Events[0].Version);

                Console.WriteLine("Etag: {0}, Version: {1}",
                                   result.Stream.ETag, result.Stream.Version);

                stream = result.Stream;
            }
        }

        static Event Payload(object e)
        {
            var id = Guid.NewGuid();

            return new Event(id.ToString("D"), new Dictionary<string, EntityProperty>
            {
                {"Id",          new EntityProperty(id)},
                {"Type",        new EntityProperty(e.GetType().Name)}, // you can include any number of custom properties along with event
                {"Data",        new EntityProperty(JSON(e))},          // you're free to choose any name you like for data property
                {"Data_Binary", new EntityProperty(BSON(e))}           // and any storage format: binary, string, whatever (any EdmType)
            });
        }

        static string JSON(object data)
        {
            return JsonConvert.SerializeObject(data);
        }

        static byte[] BSON(object data)
        {
            var ms = new System.IO.MemoryStream();
            
            using (var writer = new BsonWriter(ms))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, data);
            }

            return ms.ToArray();
        }
    }
}
