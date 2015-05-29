using System;
using System.Linq;

using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    using Streamstone;

    public class S06_Include_additional_entities : Scenario
    {
        public override void Run()
        {
            var stream = new Stream(Partition);

            Console.WriteLine("Writing to new stream along with making snapshot in partition '{0}'", 
                              stream.Partition);

            var events = new[]
            {
                Event(new InventoryItemCreated(Id, "iPhone6")),
                Event(new InventoryItemCheckedIn(Id, 100)),
                Event(new InventoryItemCheckedOut(Id, 50)),
                Event(new InventoryItemRenamed(Id, "iPhone6", "iPhone7")),
                Event(new InventoryItemCheckedOut(Id, 40))
            };

            var snapshot = Include.InsertOrReplace(new InventoryItemShapshot
            {
                RowKey  = "SNAPSHOT",
                Name    = "iPhone7",
                Count   = 100 - 50 - 40,
                Version = events.Length
            });

            var result = Stream.Write(stream, events, new[]{snapshot});

            Console.WriteLine("Succesfully written to new stream.\r\nEtag: {0}, Version: {1}",
                              result.Stream.ETag, result.Stream.Version);
        }

        static Event Event(object e)
        {
            var id = Guid.NewGuid();

            var data = new
            {
                Type = e.GetType().Name,
                Data = JsonConvert.SerializeObject(e)
            };

            return new Event(id.ToString("D"), data.Props());
        }

        class InventoryItemShapshot : TableEntity
        {
            public string Name { get; set; }
            public int Count   { get; set; }
            public int Version { get; set; }
        }
    }
}
