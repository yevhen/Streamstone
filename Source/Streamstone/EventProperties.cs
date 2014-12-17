using System;
using System.Linq;
using System.Collections.Generic;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed class EventProperties : PropertyMap
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

        public static EventProperties From(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties.Clone());
        }

        static EventProperties Build(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            return new EventProperties(properties
                .Where(x => !IsReserved(x.Key))
                .ToDictionary(p => p.Key, p => new Property(p.Value))
            );
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
