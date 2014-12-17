using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    [DebuggerDisplay("{EdmType}:{AsObject}")]
    public sealed class Property : IEquatable<Property>
    {
        readonly EntityProperty property;

        internal Property(EntityProperty property)
        {
            this.property = property;
        }

        public Property(byte[] input)
            : this(new EntityProperty(input))
        {}

        public Property(bool? input)
            : this(new EntityProperty(input))
        {}

        public Property(DateTimeOffset? input)
            : this(new EntityProperty(input))
        {}

        public Property(DateTime? input)
            : this(new EntityProperty(input))
        {}

        public Property(double? input)
            : this(new EntityProperty(input))
        {}

        public Property(Guid? input)
            : this(new EntityProperty(input))
        {}

        public Property(int? input)
            : this(new EntityProperty(input))
        {}

        public Property(long? input)
            : this(new EntityProperty(input))
        {}

        public Property(string input)
            : this(new EntityProperty(input))
        {}

        public EdmType EdmType
        {
            get { return property.PropertyType; }
        }

        public object AsObject
        {
            get { return property.PropertyAsObject; }
        }

        public byte[] BinaryValue
        {
            get { return property.BinaryValue; }
        }

        public bool? BooleanValue
        {
            get { return property.BooleanValue; }
        }

        public DateTimeOffset? DateTimeOffsetValue
        {
            get { return property.DateTimeOffsetValue; }
        }

        public double? DoubleValue
        {
            get { return property.DoubleValue; }
        }

        public Guid? GuidValue
        {
            get { return property.GuidValue; }
        }

        public int? Int32Value
        {
            get { return property.Int32Value; }
        }

        public long? Int64Value
        {
            get { return property.Int64Value; }
        }

        public string StringValue
        {
            get { return property.StringValue; }
        }

        public bool Equals(Property other)
        {
            return !ReferenceEquals(null, other) && (ReferenceEquals(this, other) || property.Equals(other.property));
        }

        public override bool Equals(object obj)
        {
            return !ReferenceEquals(null, obj) && (ReferenceEquals(this, obj) || obj is Property && Equals((Property) obj));
        }

        public override int GetHashCode()
        {
            return property.GetHashCode();
        }

        public static bool operator ==(Property left, Property right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Property left, Property right)
        {
            return !Equals(left, right);
        }

        public KeyValuePair<string, EntityProperty> PairWith(string key)
        {
            return new KeyValuePair<string, EntityProperty>(key, property);
        }
    }
}
