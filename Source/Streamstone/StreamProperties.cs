using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    sealed class StreamProperties : PropertyMap
    {
        public static readonly StreamProperties None = new StreamProperties();

        StreamProperties()
        {}
        
        StreamProperties(IDictionary<string, Property> properties) 
            : base(properties)
        {}

        internal static StreamProperties ReadEntity(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties);
        }

        public static StreamProperties From(ITableEntity entity)
        {
            Requires.NotNull(entity, "entity");
            return Build(entity.WriteEntity(new OperationContext()));
        }
        
        public static StreamProperties From(object obj)
        {
            Requires.NotNull(obj, "obj");
            return Build(TableEntity.WriteUserObject(obj, new OperationContext()));
        }

        public static StreamProperties From(IDictionary<string, Property> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties);
        }

        public static StreamProperties From(IDictionary<string, EntityProperty> properties)
        {
            Requires.NotNull(properties, "properties");
            return Build(properties.Clone());
        }

        static StreamProperties Build(IEnumerable<KeyValuePair<string, Property>> properties)
        {
            return new StreamProperties(Filter(properties).ToDictionary(p => p.Key, p => p.Value));
        }

        static StreamProperties Build(IEnumerable<KeyValuePair<string, EntityProperty>> properties)
        {
            return new StreamProperties(Filter(properties).ToDictionary(p => p.Key, p => new Property(p.Value)));
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
                case "Start":
                case "Count":
                case "Version":
                    return true;
                default:
                    return false;
            }
        }
    }
}