using System;
using System.Linq;
using System.Collections.Generic;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    sealed class EventProperties : PropertyMap
    {
        public static readonly EventProperties None = new EventProperties();

        EventProperties()
        {}

        EventProperties(IDictionary<string, Property> properties)
            : base(properties)
        {}

        internal static EventProperties ReadEntity(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties);
        }

        public static EventProperties From(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return Build(entity.WriteEntity(new OperationContext()));
        }

        public static EventProperties From(object obj)
        {
            Requires.NotNull(obj, "obj");
            return Build(TableEntity.WriteUserObject(obj, new OperationContext()));
        }

        public static EventProperties From(IDictionary<string, Property> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties);
        }

        public static EventProperties From(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties.Clone());
        }

        static EventProperties Build(IEnumerable<KeyValuePair<string, Property>> properties)
        {
            return new EventProperties(Filter(properties).ToDictionary(p => p.Key, p => p.Value));
        }

        static EventProperties Build(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            return new EventProperties(Filter(properties).ToDictionary(p => p.Key, p => new Property(p.Value)));
        }

        static IEnumerable<KeyValuePair<string, TProperty>> Filter<TProperty>(
               IEnumerable<KeyValuePair<string, TProperty>> properties)
        {
            return properties.Where(x => !IsReserved(x.Key));
        }

        static bool IsReserved(string propertyName)
        {
            switch (propertyName)
            {
                case "PartitionKey":
                case "RowKey":
                case "ETag":
                case "Timestamp":
                case "Id":
                case "Version":
                    return true;
                default:
                    return false;
            }
        }
    }
}
