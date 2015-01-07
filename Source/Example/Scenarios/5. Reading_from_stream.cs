using System;
using System.Collections.Generic;
using System.Linq;

using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class Reading_from_stream : Scenario
    {
        public override void Run()
        {
            Stream.Write(Table, new Stream(Partition), new[]
            {
                CreateEvent("e1", "Created", "{\"foo\"=\"bar\"}"),
                CreateEvent("e2", "Changed", "{\"foo\"=\"baz\"}")
            });

            // read slice from existing stream
            var slice = Stream.Read<EventEntity>(Table, Partition, startVersion: 2, sliceSize: 2);
            foreach (var @event in slice.Events)
                Console.WriteLine("{0}: {1}-{2}", @event.Version, @event.Type, @event.Data);

            // read all events in a stream
            int nextSliceStart = 1;
            do
            {
                slice = Stream.Read<EventEntity>(Table, Partition, nextSliceStart, sliceSize: 1);

                foreach (var @event in slice.Events)
                    Console.WriteLine("{0}: {1}-{2}", @event.Version, @event.Type, @event.Data);

                nextSliceStart = slice.NextEventNumber;
            }
            while (!slice.IsEndOfStream);
        }

        static Event CreateEvent(string id, string type, string data)
        {
            return new Event(id, new Dictionary<string, EntityProperty>
            {
                {"Id",   new EntityProperty(id)},
                {"Type", new EntityProperty(type)},
                {"Data", new EntityProperty(data)}
            });
        }

        class EventEntity : TableEntity
        {
            public string Type { get; set; }
            public string Data { get; set; }
            public int Version { get; set; }
        }
    }
}
