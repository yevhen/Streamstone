using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed class StreamProperties : PropertyMap
    {
        public static readonly StreamProperties None = new StreamProperties();

        StreamProperties()
        {}

        StreamProperties(IDictionary<string, EntityProperty> properties) 
            : base(properties)
        {}

        public static StreamProperties ReadEntity(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties);
        }

        public static StreamProperties From(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(Clone(properties));
        }

        public static StreamProperties From(object properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(ToDictionary(properties));
        }

        public static StreamProperties From(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return Build(ToDictionary(entity));
        }

        static StreamProperties Build(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            return new StreamProperties(properties.Where(x => !IsReserved(x.Key)).ToDictionary(p => p.Key, p => p.Value));
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