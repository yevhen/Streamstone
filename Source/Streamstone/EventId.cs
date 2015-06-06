using System;
using System.Linq;

namespace Streamstone
{
    public struct EventId : IEquatable<EventId>
    {
        public static readonly EventId None = new EventId();

        public readonly string Value;

        public static EventId From(string id)
        {
            Requires.NotNull(id, "id");
            return new EventId(id);
        }

        public static EventId From(Guid id, string format = "D")
        {
            return new EventId(id.ToString(format));
        }

        EventId(string value)
        {
            Value = value;
        }

        public bool Equals(EventId other)
        {
            return string.Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            return !ReferenceEquals(null, obj) && (obj is EventId && Equals((EventId) obj));
        }

        public override int GetHashCode()
        {
            return (Value != null ? Value.GetHashCode() : 0);
        }

        public static bool operator ==(EventId left, EventId right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(EventId left, EventId right)
        {
            return !left.Equals(right);
        }

        public static implicit operator string(EventId arg)
        {
            return arg.Value;
        }
    }
}
