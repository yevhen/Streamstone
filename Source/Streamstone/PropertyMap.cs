using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public abstract class PropertyMap : IEnumerable<KeyValuePair<string, EntityProperty>>
    {
        readonly IDictionary<string, EntityProperty> properties = new Dictionary<string, EntityProperty>();

        protected PropertyMap()
        {}

        protected PropertyMap(IDictionary<string, EntityProperty> properties)
        {
            this.properties = properties;
        }

        public IEnumerator<KeyValuePair<string, EntityProperty>> GetEnumerator()
        {
            return properties.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)properties).GetEnumerator();
        }

        internal void WriteTo(IDictionary<string, EntityProperty> target)
        {
            foreach (var property in properties)
                target.Add(property.Key, property.Value);
        }

        internal static IEnumerable<KeyValuePair<string, EntityProperty>> ToDictionary(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return entity.WriteEntity(new OperationContext());
        }

        static readonly object[] noargs = new object[0];

        protected static IEnumerable<KeyValuePair<string, EntityProperty>> ToDictionary(object obj)
        {
            Requires.NotNull(obj, "obj");

            return from property in obj.GetType().GetProperties() 
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
