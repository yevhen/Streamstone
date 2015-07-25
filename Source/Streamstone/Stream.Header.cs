using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public readonly StreamProperties Properties;
        public readonly Partition Partition;
        public readonly string ETag;
        public readonly int Version;

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        public Stream(Partition partition) 
            : this(partition, StreamProperties.None)
        {}

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance with the given additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <param name="properties">
        /// The additional properties for this stream.
        /// </param>
        public Stream(Partition partition, StreamProperties properties)
        {
            Requires.NotNull(partition, "partition");
            Requires.NotNull(properties, "properties");

            Partition = partition;
            Properties = properties;
        }

        internal Stream(Partition partition, string etag, int version, StreamProperties properties)
        {
            Partition = partition;
            ETag = etag;
            Version = version;
            Properties = properties;
        }

        public bool IsTransient
        {
            get { return ETag == null; }
        }

        public bool IsPersistent
        {
            get { return !IsTransient; }
        }

        static Stream From(Partition partition, StreamEntity entity)
        {
            return new Stream(partition, entity.ETag, entity.Version, entity.Properties);
        }

        StreamEntity Entity()
        {
            return new StreamEntity(Partition, ETag, Version, Properties);
        }

        IEnumerable<RecordedEvent> Record(IEnumerable<EventData> events)
        {
            return events.Select((e, i) => e.Record(Partition, Version + i + 1));
        }
    }
}
