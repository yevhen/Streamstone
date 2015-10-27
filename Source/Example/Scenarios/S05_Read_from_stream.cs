using System;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;
using Streamstone;

namespace Example.Scenarios
{
    public class S05_Read_from_stream : Scenario
    {
        public override void Run()
        {
            Prepare();

            ReadSlice();
            ReadAll();
        }

        void Prepare()
        {
            var events = Enumerable
                .Range(1, 10)
                .Select(Event)
                .ToArray();

            var existent = Stream.TryOpen(Partition);
	        var stream = existent.Found ? existent.Stream : new Stream(Partition);
	        Stream.Write(stream, events);
        }

        void ReadSlice()
        {
            Console.WriteLine("Reading single slice from specified start version and using specified slice size");

            var slice = Stream.Read<EventEntity>(Partition, startVersion: 2, sliceSize: 2);
            foreach (var @event in slice.Events)
                Console.WriteLine("{0}: {1}-{2}", @event.Version, @event.Type, @event.Data);

            Console.WriteLine();
        }

        void ReadAll()
        {
            Console.WriteLine("Reading all events in a stream");
            Console.WriteLine("If slice size is > than WATS limit, continuation token will be managed automatically");

            StreamSlice<EventEntity> slice;
            int nextSliceStart = 1;

            do
            {
                slice = Stream.Read<EventEntity>(Partition, nextSliceStart, sliceSize: 1);

                foreach (var @event in slice.Events)
                    Console.WriteLine("{0}:{1} {2}-{3}", @event.Id, @event.Version, @event.Type, @event.Data);

                nextSliceStart = slice.NextEventNumber;
            }
            while (!slice.IsEndOfStream);
        }

        static EventData Event(int id)
        {
            var properties = new
            {
                Id = id,
                Type = "<type>",
                Data = "{some}"
            };

            return new EventData(EventId.From(id.ToString()), EventProperties.From(properties));
        }

        class EventEntity
        {
            public int Id { get; set; }
            public string Type { get; set; }
            public string Data { get; set; }
            public int Version { get; set; }
        }
    }
}
