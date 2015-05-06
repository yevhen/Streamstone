using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Streamstone;

namespace Example.Scenarios
{
    public class S06_Include_additional_entities : Scenario
    {
        public override void Run()
        {
            var stream = new Stream(Partition);

            Console.WriteLine("Writing to new stream along with making snapshot in partition '{0}'", stream.Partition);

            var events = new[]
            {
                Event(new InventoryItemCreated(Partition, "iPhone6")),
                Event(new InventoryItemCheckedIn(Partition, 100)),
                Event(new InventoryItemCheckedOut(Partition, 50)),
                Event(new InventoryItemRenamed(Partition, "iPhone6", "iPhone7")),
                Event(new InventoryItemCheckedOut(Partition, 40))
            };

            var shapshot = new InventoryItemShapshot
            {
                RowKey = "SNAPSHOT",
                Name   = "iPhone7",
                Count  = 100 - 50 - 40,
                Version = events.Length
            };

            var includes = new[]
            {
                Include.InsertOrReplace(shapshot)
            };

            var result = Stream.Write(Table, stream, events, includes);

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
