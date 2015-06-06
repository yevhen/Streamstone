using System;
using System.Linq;

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
        public string Id
        {
            get; private set;
        }

        /// <summary>
        /// The readonly map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public EventProperties Properties
        {
            get; private set;
        }

        /// <summary>
        /// Additional entity includes to be stored along with this event
        /// </summary>
        public Include[] Includes
        {
            get; private set;
        }

        static readonly Include[] NoIncludes = new Include[0];

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for duplicate event detection).
        /// </param>
        public EventData(string id)
            : this(id, EventProperties.None, NoIncludes)
        {}

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance which doesn't have any additional properties 
        /// but includes a set of additional entity includes.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for duplicate event detection).
        /// </param>
        /// <param name="includes">
        /// Additional entity includes to be stored along with this event
        ///  </param>
        public EventData(string id, Include[] includes) 
            : this(id, EventProperties.None, includes)
        {}

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance using given event properties.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for duplicate event detection).
        /// </param>
        /// <param name="properties">
        /// The properties for this event (includes both meta and data properties).
        /// </param>
        public EventData(string id, EventProperties properties)
            : this(id, properties, NoIncludes)
        {}

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance using given event properties
        /// and additional entity includes.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for duplicate event detection).
        /// </param>
        /// <param name="properties">
        /// The properties for this event (includes both meta and data properties).
        /// </param>
        /// <param name="includes">
        /// Additional entity includes to be stored along with this event
        ///  </param>
        public EventData(string id, EventProperties properties, Include[] includes)
        {
            Requires.NotNull(properties, "properties");
            Requires.NotNull(includes, "includes");
            
            Id = id;
            Includes = includes;
            Properties = properties;
        }

        internal RecordedEvent Record(int version)
        {
            return new RecordedEvent(Id, Properties, Includes, version);
        }
    }

    /// <summary>
    /// Represents a previously written event
    /// </summary>
    public sealed class RecordedEvent
    {
        /// <summary>
        /// The unique identifier representing this event
        /// </summary>
        public string Id
        {
            get; private set;
        }

        /// <summary>
        /// The readonly map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public EventProperties Properties
        {
            get; private set;
        }

        /// <summary>
        /// Additional entity includes that were stored along with this event
        /// </summary>
        public Include[] Includes
        {
            get; private set;
        }

        /// <summary>
        /// A sequence number assigned by a stream to this event. 
        /// </summary>
        public int Version
        {
            get; private set;
        }

        internal RecordedEvent(string id, EventProperties properties, Include[] includes, int version)
        {
            Id = id;
            Version = version;
            Includes = includes;
            Properties = properties;
        }

        internal EventEntity EventEntity(Partition partition)
        {
            return new EventEntity(partition, Version, Properties);
        }

        internal EventIdEntity IdEntity(Partition partition)
        {
            return new EventIdEntity(partition, Id, Version);
        }
    }
}