using System;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represents an event to be written.
    /// </summary>
    public sealed class Event
    {
        /// <summary>
        /// The unique identifier representing this event
        /// </summary>
        public readonly string Id;

        /// <summary>
        /// The named map of additional properties which this event contains.
        /// Includes both meta and data properties.
        /// </summary>
        public readonly EventProperties Properties;

        /// <summary>
        /// Constructs a new <see cref="Event"/> class.
        /// </summary>
        /// <param name="id">The unique identifier of the event (used for idempotent writes).</param>
        /// <param name="properties">The properties of the event (includes both meta and data properties).</param>
        public Event(string id, EventProperties properties)
        {
            Requires.NotNullOrWhitespace(id, "id");
            Requires.NotNull(properties, "properties");

            Id = id;
            Properties = properties;
        }
    }

    /// <summary>
    /// Represents a previously written (stored) event
    /// </summary>
    public sealed class StoredEvent
    {
        /// <summary> 
        /// The unique identifier representing this event
        ///  </summary>
        public readonly string Id;

        /// <summary>  
        /// The named map of additional properties which this event contains.  
        /// </summary>
        /// <remarks> Includes both meta and data properties. </remarks>
        public readonly EventProperties Properties;

        /// <summary>
        /// A sequence number assigned by a stream to this event. 
        /// </summary>
        public readonly int Version;

        internal StoredEvent(string id, int version, EventProperties properties)
        {
            Id = id;
            Properties = properties;
            Version = version;
        }
    }
}