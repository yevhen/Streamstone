using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    /// <summary>
    /// Represents an event to be written.
    /// </summary>
    public sealed class EventData
    {
        /// <summary>
        /// The unique identifier representing this event
        /// </summary>
        public readonly string Id;

        readonly EventProperties properties;
        
        /// <summary>
        /// The readonly map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public IEnumerable<KeyValuePair<string, EntityProperty>> Properties 
        {
            get { return properties; }
        }

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance which doesn't have any additional properties.
        /// </summary>
        public EventData(string id) 
            : this(id, EventProperties.None)
        {}

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance using given properties.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for idempotent writes).
        /// </param>
        /// <param name="properties">
        /// The properties for this event (includes both meta and data properties).
        /// </param>
        public EventData(string id, IDictionary<string, EntityProperty> properties)
            : this(id, EventProperties.From(properties))
        {}

        EventData(string id, EventProperties properties)
        {
            Id = id;
            this.properties = properties;
        }

        internal RecordedEvent Record(int version)
        {
            return new RecordedEvent(Id, version, properties);
        }
    }

    /// <summary>
    /// Represents a previously written event
    /// </summary>
    public sealed class RecordedEvent
    {
        readonly EventProperties properties;

        /// <summary>
        /// The unique identifier representing this event
        /// </summary>
        public readonly string Id;

        /// <summary>
        /// The readonly map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public IEnumerable<KeyValuePair<string, EntityProperty>> Properties
        {
            get { return properties; }
        }

        /// <summary>
        /// A sequence number assigned by a stream to this event. 
        /// </summary>
        public readonly int Version;

        internal RecordedEvent(string id, int version, EventProperties properties)
        {
            Id = id;
            Version = version;
            this.properties = properties;
        }

        internal EventEntity EventEntity(Partition partition)
        {
            return new EventEntity(partition, Version, properties);
        }

        internal EventIdEntity IdEntity(Partition partition)
        {
            return new EventIdEntity(partition, Id, Version);
        }
    }
}