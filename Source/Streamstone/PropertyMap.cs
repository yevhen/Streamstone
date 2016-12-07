using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Reflection;

namespace Streamstone
{
    /// <summary>
    /// Represents collection of named properties
    /// </summary>
    public abstract class PropertyMap : IEnumerable<KeyValuePair<string, EntityProperty>>
    {
        readonly IDictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();

        /// <summary>
        /// Initializes a new instance of the <see cref="PropertyMap"/> class.
        /// </summary>
        protected PropertyMap()
        {}

        /// <summary>
        /// Initializes a new instance of the <see cref="PropertyMap"/> class.
        /// </summary>
        /// <param name="properties">The properties.</param>
        protected PropertyMap(IDictionary<string, EntityProperty> properties)
        {
            this.properties = properties;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<KeyValuePair<string, EntityProperty>> GetEnumerator()
        {
            return properties.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)properties).GetEnumerator();
        }

        /// <summary>
        ///  Gets property with specified key.
        /// </summary>
        /// <param name="key">The key of the property to get</param>
        /// <returns>
        ///  The element with the specified key.
        /// </returns>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.</exception>
        /// <exception cref="T:System.Collections.Generic.KeyNotFoundException">The property is retrieved and <paramref name="key"/> is not found.</exception>
        /// <filterpriority>1</filterpriority>
        public EntityProperty this[string key] => properties[key];

        /// <summary>
        ///  Determines whether the <see cref="PropertyMap"/> contains a property with the specified key.
        /// </summary>
        /// <returns>
        ///  <c>true</c> if the <see cref="PropertyMap"/> contains an element with the key; otherwise, <c>false</c>.
        /// </returns>
        /// <param name="key">The key to locate in the <see cref="PropertyMap"/>.</param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="key"/> is null.</exception>
        public bool ContainsKey(string key) => properties.ContainsKey(key);

        internal void WriteTo(IDictionary<string, EntityProperty> target)
        {
            foreach (var property in properties)
                target.Add(property.Key, property.Value);
        }

        internal static IEnumerable<KeyValuePair<string, EntityProperty>> ToDictionary(ITableEntity entity)
        {
            return entity.WriteEntity(new OperationContext());
        }

        static readonly object[] noargs = new object[0];

        /// <summary>
        /// Converts given object instance to a sequence of named properties. 
        /// Only public properties of WATS compatible types will be converted.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>Sequence of named propeties</returns>
        /// <exception cref="ArgumentNullException">
        ///     If <paramref name="obj"/> is <c>null</c>
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     If <paramref name="obj"/> has properties of WATS incompatible type
        /// </exception>
        protected static IEnumerable<KeyValuePair<string, EntityProperty>> ToDictionary(object obj)
        {
            Requires.NotNull(obj, "obj");

            return from property in obj.GetType().GetTypeInfo().GetProperties() 
                   let key = property.Name 
                   let value = property.GetValue(obj, noargs) 
                   select ToKeyValuePair(key, value, property.PropertyType);
        }

        static KeyValuePair<string, EntityProperty> ToKeyValuePair(string key, object value, Type type)
        {
            return new KeyValuePair<string, EntityProperty>(key, ToEntityProperty(key, value, type));
        }

        static EntityProperty ToEntityProperty(string key, object value, Type type)
        {
            if (type == typeof(byte[]))
                return new EntityProperty((byte[])value);

            if (type == typeof(bool) || type == typeof(bool?))
                return new EntityProperty((bool?)value);

            if (type == typeof(DateTimeOffset) || type == typeof(DateTimeOffset?))
                return new EntityProperty((DateTimeOffset?)value);

            if (type == typeof(DateTime))
                return new EntityProperty((DateTime?)value);

            if (type == typeof(double) || type == typeof(double?))
                return new EntityProperty((double?)value);

            if (type == typeof(Guid) || type == typeof(Guid?))
                return new EntityProperty((Guid?)value);

            if (type == typeof(int) || type == typeof(int?))
                return new EntityProperty((int?)value);

            if (type == typeof(long) || type == typeof(long?))
                return new EntityProperty((long?)value);

            if (type == typeof(string))
                return new EntityProperty((string)value);

            throw new NotSupportedException("Not supported entity property type '" + value.GetType() + "' for '" + key + "'");
        }

        internal static IEnumerable<KeyValuePair<string, EntityProperty>> Clone(
                        IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            return properties.Select(Clone);
        }

        static KeyValuePair<string, EntityProperty> Clone(KeyValuePair<string, EntityProperty> x)
        {
            return new KeyValuePair<string, EntityProperty>(x.Key, Clone(x.Value));
        }

        static EntityProperty Clone(EntityProperty source)
        {
            return EntityProperty.CreateEntityPropertyFromObject(source.PropertyAsObject);
        }
    }
}
