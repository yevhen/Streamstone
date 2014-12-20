using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public readonly string Partition;
        public readonly string ETag;
        public readonly int Start;
        public readonly int Count;
        public readonly int Version;

        readonly StreamProperties properties;
        
        /// <summary>
        /// The readonly map of additional properties which this stream has.
        /// </summary>
        public IEnumerable<KeyValuePair<string, Property>> Properties
        {
            get { return properties; }
        }

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition key in which this stream will reside. 
        /// </param>
        public Stream(string partition) 
            : this(partition, StreamProperties.None)
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with additional properties from given <see cref="ITableEntity"/>.
        /// </summary>
        /// <param name="partition">
        /// The partition key in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The instance of <see cref="ITableEntity"/> which contains additional properties for this stream.
        /// </param>
        public Stream(string partition, ITableEntity properties)
            : this(partition, StreamProperties.From(properties))
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with additional properties from given object.
        /// </summary>
        /// <param name="partition">
        /// The partition key in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The object which contains additional properties for this stream.
        /// </param>
        public Stream(string partition, object properties)
            : this(partition, StreamProperties.From(properties))
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with the given additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition key in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The additional properties for this stream.
        /// </param>
        public Stream(string partition, IDictionary<string, Property> properties)
            : this(partition, StreamProperties.From(properties))
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with the given additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition key in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The additional properties for this stream.
        /// </param>
        public Stream(string partition, IDictionary<string, EntityProperty> properties)
            : this(partition, StreamProperties.From(properties))
        {}

        Stream(string partition, StreamProperties properties)
        {
            Requires.NotNullOrEmpty(partition, "partition");
            Requires.NotNull(properties, "properties");

            Partition = partition;
            this.properties = properties;
        }

        internal Stream(
            string partition,
            StreamProperties properties,
            string etag,
            int start,
            int count,
            int version)
        {
            Partition = partition;
            this.properties = properties;
            ETag = etag;
            Start = start;
            Count = count;
            Version = version;
        }
        
        bool IsTransient
        {
            get { return ETag == null; }
        }

        bool IsStored
        {
            get { return !IsTransient; }
        }

        Stream SetProperties(StreamProperties properties)
        {
            return new Stream(Partition, properties, ETag, Start, Count, Version);
        }

        WriteTransaction Write(ICollection<Event> events, Include[] includes)
        {
            var start = Start == 0 ? (events.Count != 0 ? 1 : 0) : Start;
            var count = Count + events.Count;
            var version = Version + events.Count;

            var transient = events
                .Select((e, i) => new TransientEvent(e, Version + i + 1, Partition))
                .ToArray();

            return new WriteTransaction(new Stream(Partition, properties, ETag, start, count, version), transient, includes);
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
                properties,
                ETag,
                Start,
                Count,
                Version
            );
        }

        class WriteTransaction
        {
            public readonly Stream Stream;
            public readonly TransientEvent[] Events;
            public readonly Include[] Includes;

            public WriteTransaction(Stream stream, TransientEvent[] events, Include[] includes)
            {
                Stream = stream;
                Events = events;
                Includes = includes;
            }
        }

        class TransientEvent
        {
            public readonly Event Source;
            public readonly int Version;
            public readonly string Partition;

            internal TransientEvent(Event source, int version, string partition)
            {
                Source = source;
                Version = version;
                Partition = partition;
            }

            internal EventEntity EventEntity()
            {
                return Source.Entity(Partition, Version);
            }

            internal EventIdEntity IdEntity()
            {
                return new EventIdEntity(Partition, Source.Id, Version);
            }

            public StoredEvent Stored()
            {
                return Source.Stored(Version);
            }
        }
    }
}
