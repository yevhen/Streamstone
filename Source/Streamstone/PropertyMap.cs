using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    abstract class PropertyMap : IEnumerable<KeyValuePair<string, EntityProperty>>
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
    }
}
