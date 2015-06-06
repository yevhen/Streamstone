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
        public EventId Id
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
        public EventIncludes Includes
        {
            get; private set;
        }

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance which doesn't have any additional properties, includes and id.
        /// </summary>
        public EventData()
            : this(EventId.None, EventProperties.None, EventIncludes.None)
        {}

        /// <summary>
        /// Constructs a new <see cref="EventData"/> instance which doesn't have any additional properties.
        /// </summary>
        /// <param name="id">
        /// The unique identifier of the event (used for duplicate event detection).
        /// </param>
        public EventData(EventId id)
            : this(id, EventProperties.None, EventIncludes.None)
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
        public EventData(EventId id, EventIncludes includes)
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
        public EventData(EventId id, EventProperties properties)
            : this(id, properties, EventIncludes.None)
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
        public EventData(EventId id, EventProperties properties, EventIncludes includes)
        {
            Requires.NotNull(properties, "properties");
            Requires.NotNull(includes, "includes");
            
            Id = id;
            Includes = includes;
            Properties = properties;
        }

        internal RecordedEvent Record(Partition partition, int version)
        {
            return new RecordedEvent(Id, Properties, Includes, partition, version);
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
        public EventId Id
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
        public EventIncludes Includes
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

        internal readonly EntityOperation[] Operations;

        internal RecordedEvent(EventId id, EventProperties properties, EventIncludes includes, Partition partition, int version)
        {
            Id = id;
            Version = version;
            Includes = includes;
            Properties = properties;
            Operations = Prepare(partition).ToArray();
        }

        IEnumerable<EntityOperation> Prepare(Partition partition)
        {
            yield return EventEntity(partition);

            if (Id != EventId.None)
                yield return IdEntity(partition);

            foreach (var include in Includes.Select(x => x.Apply(partition)))
                yield return include;
        }

        EntityOperation EventEntity(Partition partition)
        {
            var entity = new EventEntity(partition, this);
            return new EntityOperation(entity, TableOperation.Insert(entity));
        }

        EntityOperation IdEntity(Partition partition)
        {
            var entity = new EventIdEntity(partition, this);
            return new EntityOperation(entity, TableOperation.Insert(entity));
        }
    }
}