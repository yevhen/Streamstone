using System;
using System.Collections.Generic;
using System.Linq;

using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class S5_Read_from_stream : Scenario
    {
        public override void Run()
        {
            Prepare();

            ReadSingleSlice();
            ReadAllEvents();
        }

        void Prepare()
        {
            var events = Enumerable
                .Range(1, 10)
                .Select(Payload)
                .ToArray();

            Stream.Write(Table, new Stream(Partition), events);
        }

        void ReadSingleSlice()
        {
            Console.WriteLine("Reading single slice from specified start version and using specified slice size");

            var slice = Stream.Read<EventEntity>(Table, Partition, startVersion: 2, sliceSize: 2);
            foreach (var @event in slice.Events)
                Console.WriteLine("{0}: {1}-{2}", @event.Version, @event.Type, @event.Data);

            Console.WriteLine();
        }

        void ReadAllEvents()
        {
            Console.WriteLine("Reading all events in a stream");
            Console.WriteLine("If slice size is > than WATS limit, continuation token will be managed automatically");

            StreamSlice<EventEntity> slice;
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

        static Event Payload(int id)
        {
            return new Event(id.ToString(), new Dictionary<string, EntityProperty>
            {
                {"Id",   new EntityProperty(id)},
                {"Type", new EntityProperty("<type>")},
                {"Data", new EntityProperty("{some}")}
            });
        }

        /// define entity that will hold event properties
        class EventEntity : TableEntity     
        {
            public string Type { get; set; }
            public string Data { get; set; }
            public int Version { get; set; }
        }
    }
}
