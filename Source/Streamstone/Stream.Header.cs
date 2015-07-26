using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represents an event stream. Instances of this class enapsulate stream header information such as version, etag,  metadata, etc;
    /// while static methods are used to manipulate stream.
    /// </summary>
    public sealed partial class Stream
    {
        /// <summary>
        /// The additional properties (metadata) of this stream
        /// </summary>
        public readonly StreamProperties Properties;

        /// <summary>
        /// The partition in which this stream resides.
        /// </summary>
        public readonly Partition Partition;
        
        /// <summary>
        /// The latest etag
        /// </summary>
        public readonly string ETag;
        
        /// <summary>
        /// The version of the stream. Sequential, monotonically increasing, no gaps.
        /// </summary>
        public readonly int Version;

        /// <summary>
        /// Constructs a new <see cref="Stream"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="partition">
        /// The partition in which this stream will reside. 
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
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
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="partition"/> is <c>null</c>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
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

        /// <summary>
        /// Gets a value indicating whether this stream header represents a transient stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this stream header was newed; otherwise, <c>false</c>.
        /// </value>
        public bool IsTransient
        {
            get { return ETag == null; }
        }

        /// <summary>
        /// Gets a value indicating whether this stream header represents a persistent stream.
        /// </summary>
        /// <value>
        /// <c>true</c> if this stream header has been obtained from storage; otherwise, <c>false</c>.
        /// </value>
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
