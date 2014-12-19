using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    /// <summary>
    /// Represents an event to be written.
    /// </summary>
    public sealed class Event
    {
        internal readonly EventProperties PropertiesInternal;

        /// <summary>
        /// The unique identifier representing this event
        /// </summary>
        public readonly string Id;

        /// <summary>
        /// The map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public IEnumerable<KeyValuePair<string, Property>> Properties 
        {
            get { return PropertiesInternal; }
        }

        /// <summary>
        /// Constructs a new <see cref="Event"/> instance which doesn't have any additional properties.
        /// </summary>
        public Event(string id) 
            : this(id, EventProperties.None)
        {}

        /// <summary>
        /// Constructs a new <see cref="Event"/> instance using properties from given <see cref="ITableEntity"/>.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for idempotent writes).
        /// </param>
        /// <param name="properties">
        /// The instance of <see cref="ITableEntity"/> which contains properties for this event (includes both meta and data properties).
        /// </param>
        public Event(string id, ITableEntity properties)
            : this(id, EventProperties.From(properties))
        {}

        /// <summary>
        /// Constructs a new <see cref="Event"/> instance using properties from given object.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for idempotent writes).
        /// </param>
        /// <param name="properties">
        /// The object which contains properties for this event (includes both meta and data properties).
        /// </param>
        public Event(string id, object properties)
            : this(id, EventProperties.From(properties))
        {}

        /// <summary>
        /// Constructs a new <see cref="Event"/> instance using given properties.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for idempotent writes).
        /// </param>
        /// <param name="properties">
        /// The properties for this event (includes both meta and data properties).
        /// </param>
        public Event(string id, IDictionary<string, Property> properties)
            : this(id, EventProperties.From(properties))
        {}

        /// <summary>
        /// Constructs a new <see cref="Event"/> instance using given properties.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for idempotent writes).
        /// </param>
        /// <param name="properties">
        /// The properties for this event (includes both meta and data properties).
        /// </param>
        public Event(string id, IDictionary<string, EntityProperty> properties)
            : this(id, EventProperties.From(properties))
        {}

        Event(string id, EventProperties properties)
        {
            Id = id;
            PropertiesInternal = properties;
        }
    }

    /// <summary>
    /// Represents a previously written (stored) event
    /// </summary>
    public sealed class StoredEvent
    {
        readonly EventProperties properties;

        /// <summary>
        /// The unique identifier representing this event
        /// </summary>
        public readonly string Id;

        /// <summary>
        /// The map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public IEnumerable<KeyValuePair<string, Property>> Properties
        {
            get { return properties; }
        }

        /// <summary>
        /// A sequence number assigned by a stream to this event. 
        /// </summary>
        public readonly int Version;

        internal StoredEvent(string id, int version, EventProperties properties)
        {
            Id = id;
            Version = version;
            this.properties = properties;
        }
    }
}