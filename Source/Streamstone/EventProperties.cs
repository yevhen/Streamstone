using System;
using System.Linq;
using System.Collections.Generic;

using Azure.Data.Tables;

namespace Streamstone
{
    /// <summary>
    /// Represents the collection of named event properties
    /// </summary>
    public sealed class EventProperties : PropertyMap
    {
        /// <summary>
        /// An empty collection of event properties
        /// </summary>
        public static readonly EventProperties None = new EventProperties();

        EventProperties()
        { }

        EventProperties(IDictionary<string, object> properties)
            : base(properties)
        { }

        public override void CopyFrom(TableEntity entity)
        {
            Clear();
            foreach (var property in Build(entity))
            {
                Add(property.Key, property.Value);
            }
        }

        internal static EventProperties ReadEntity(IDictionary<string, object> properties)
        {
            Requires.NotNull(properties, nameof(properties));
            return Build(properties);
        }

        /// <summary>
        /// Creates new instance of <see cref="EventProperties"/> class using given dictionary of entity properties
        /// </summary>
        /// <param name="properties">The properties.</param>
        /// <returns>New instance of <see cref="EventProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="properties"/> is <c>null</c>
        /// </exception>
        public static EventProperties From(IDictionary<string, object> properties)
        {
            Requires.NotNull(properties, nameof(properties));
            return Build(properties);
        }

        /// <summary>
        /// Creates new instance of <see cref="EventProperties"/> class using public properties of a given object.
        /// All public properties should be of WATS compatible type..
        /// </summary>
        /// <param name="obj">The properties.</param>
        /// <returns>New instance of <see cref="EventProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="obj"/> is <c>null</c>
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     If  <paramref name="obj"/> has properties of WATS incompatible type
        /// </exception>
        public static EventProperties From(object obj)
        {
            Requires.NotNull(obj, nameof(obj));
            return Build(ToDictionary(obj));
        }

        /// <summary>
        /// Creates new instance of <see cref="EventProperties"/> class using public properties of a given table entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>New instance of <see cref="EventProperties"/> class</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="entity"/> is <c>null</c>
        /// </exception>
        public static EventProperties From(TableEntity entity)
        {
            Requires.NotNull(entity, nameof(entity));
            return Build(entity);
        }

        static EventProperties Build(IEnumerable<KeyValuePair<string, object>> properties)
        {
            var filtered = properties
                .Where(x => !IsReserved(x.Key))
                .ToDictionary(p => p.Key, p => p.Value);

            return new EventProperties(filtered);
        }

        static bool IsReserved(string propertyName)
        {
            return propertyName 
                is nameof(EventEntity.PartitionKey) 
                or nameof(EventEntity.RowKey)
                or nameof(EventEntity.ETag) 
                or "odata.etag" 
                or nameof(EventEntity.Timestamp) 
                or nameof(EventEntity.Version);
        }
    }
}