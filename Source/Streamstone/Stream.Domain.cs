using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public readonly string Partition;
        public readonly StreamProperties Properties;
        
        public readonly string Etag;
        public readonly int Start;
        public readonly int Count;
        public readonly int Version;

        Stream(string partition, StreamProperties properties)
        {
            Partition = partition;
            Properties = properties;
        }

        internal Stream(
            string partition, 
            StreamProperties properties,
            string etag,
            int start, 
            int count, 
            int version) 
            : this(partition, properties)
        {
            Etag = etag;
            Start = start;
            Count = count;
            Version = version;
        }

        Stream SetProperties(StreamProperties properties)
        {
            return new Stream(Partition, properties, Etag, Start, Count, Version);
        }

        WriteAttempt Write(ICollection<Event> events)
        {
            var start = Start == 0 ? (events.Count != 0 ? 1 : 0) : Start;
            var count = Count + events.Count;
            var version = Version + events.Count;

            var transient = events
                .Select((e, i) => new TransientEvent(e, Version + i + 1, Partition))
                .ToArray();

            return new WriteAttempt(new Stream(Partition, Properties, Etag, start, count, version), transient);
        }

        static Stream From(StreamEntity entity)
        {
            return new Stream(
                entity.PartitionKey, 
                entity.Properties,
                entity.ETag,
                entity.Start,
                entity.Count,
                entity.Version
            );
        }

        StreamEntity Entity()
        {
            return new StreamEntity
            (
                Partition,
                Properties,
                Etag,
                Start,
                Count,
                Version
            );
        }

        class WriteAttempt
        {
            public readonly Stream Stream;
            public readonly TransientEvent[] Events;

            public WriteAttempt(Stream stream, TransientEvent[] events)
            {
                Stream = stream;
                Events = events;
            }
        }

        class TransientEvent
        {
            public readonly string Id;
            public readonly int Version;
            public readonly EventProperties Properties;
            public readonly string Partition;

            internal TransientEvent(Event e, int version, string partition)
            {
                Id = e.Id;
                Properties = e.Properties;
                Version = version;
                Partition = partition;
            }

            internal EventEntity EventEntity()
            {
                return new EventEntity(Partition, Id, Version, Properties);
            }

            internal EventIdEntity IdEntity()
            {
                return new EventIdEntity(Partition, Id, Version);
            }
        }
    }
}
