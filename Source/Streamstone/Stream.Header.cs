using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed partial class Stream
    {
        public readonly Partition Partition;
        public readonly string ETag;
        public readonly int Version;

        readonly StreamProperties properties;
        
        /// <summary>
        /// The readonly map of additional properties which this stream has.
        /// </summary>
        public IEnumerable<KeyValuePair<string, EntityProperty>> Properties
        {
            get { return properties; }
        }

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
        public Stream(Partition partition, IDictionary<string, EntityProperty> properties)
            : this(partition, StreamProperties.From(properties))
        {}

        Stream(Partition partition, StreamProperties properties)
        {
            Requires.NotNull(partition, "partition");
            Requires.NotNull(properties, "properties");

            Partition = partition;
            this.properties = properties;
        }

        internal Stream(Partition partition, string etag, int version, StreamProperties properties)
        {
            Partition = partition;
            ETag = etag;
            Version = version;
            this.properties = properties;
        }
        
        bool IsTransient
        {
            get { return ETag == null; }
        }

        static Stream From(Partition partition, StreamEntity entity)
        {
            return new Stream(partition, entity.ETag, entity.Version, entity.Properties);
        }

        StreamEntity Entity()
        {
            return new StreamEntity(Partition, ETag, Version, properties);
        }
    }
}
