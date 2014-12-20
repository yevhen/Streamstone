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
    }
}
