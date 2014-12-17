using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public abstract class PropertyMap : IEnumerable<KeyValuePair<string, Property>>
    {
        readonly IDictionary<string, Property> properties = new Dictionary<string, Property>();

        protected PropertyMap()
        {}

        protected PropertyMap(IDictionary<string, Property> properties)
        {
            this.properties = properties;
        }

        public IEnumerator<KeyValuePair<string, Property>> GetEnumerator()
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
                target.Add(property.Value.PairWith(property.Key));
        }
    }
}
