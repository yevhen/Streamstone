using System;
using System.Linq;
using System.Collections.Generic;

using Microsoft.WindowsAzure.Storage.Table;

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
        {}

        EventProperties(IDictionary<string, EntityProperty> properties)
            : base(properties)
        {}

        internal static EventProperties ReadEntity(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
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
        public static EventProperties From(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(Clone(properties));
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
            Requires.NotNull(obj, "obj");
            return Build(ToDictionary(obj));
        }

        static EventProperties Build(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            var filtered = properties
                .Where(x => !IsReserved(x.Key))
                .ToDictionary(p => p.Key, p => p.Value);

            return new EventProperties(filtered);
        }

        static bool IsReserved(string propertyName)
        {
            switch (propertyName)
            {
                case "PartitionKey":
                case "RowKey":
                case "ETag":
                case "Timestamp":
                case "Version":
                    return true;
                default:
                    return false;
            }
        }
    }
}
