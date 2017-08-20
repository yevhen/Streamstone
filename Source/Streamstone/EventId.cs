using System;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represents unique id of the event
    /// </summary>
    public struct EventId : IEquatable<EventId>
    {
        /// <summary>
        /// Value for event which has no unique id
        /// </summary>
        public static readonly EventId None = new EventId();

        /// <summary>
        /// Value of unique id
        /// </summary>
        public readonly string Value;

        /// <summary>
        /// Creates instance of  <see cref="EventId"/> from <see cref="string"/> id
        /// </summary>
        /// <param name="id">Value of new <see cref="EventId"/></param>
        /// <returns>New instance of <see cref="EventEntity"/> class</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="id"/> is <c>null</c></exception>
        public static EventId From(string id)
        {
            Requires.NotNull(id, nameof(id));
            return new EventId(id);
        }

        /// <summary>
        /// Creates instance of  <see cref="EventId"/> from <see cref="Guid"/> id
        /// </summary>
        /// <param name="id">Value of new <see cref="EventId"/></param>
        /// <param name="format">The format string. Default is "D"</param>
        /// <returns>New instance of <see cref="EventEntity"/> struct</returns>
        /// <exception cref="FormatException"> 
        /// The value of <paramref name="format"/> is not null, an empty string (""), "N", "D", "B", "P", or "X".  
        /// </exception>
        public static EventId From(Guid id, string format = "D")
        {
            return new EventId(id.ToString(format));
        }

        EventId(string value)
        {
            Value = value;
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(EventId other)
        {
            return string.Equals(Value, other.Value);
        }

        /// <summary>
        /// Indicates whether this instance and a specified object are equal.
        /// </summary>
        /// <returns>
        /// true if <paramref name="obj"/> and this instance are the same type and represent the same value; otherwise, false.
        /// </returns>
        /// <param name="obj">Another object to compare to. </param><filterpriority>2</filterpriority>
        public override bool Equals(object obj)
        {
            return !ReferenceEquals(null, obj) && (obj is EventId && Equals((EventId) obj));
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>
        /// A 32-bit signed integer that is the hash code for this instance.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return (Value != null ? Value.GetHashCode() : 0);
        }

        /// <summary>
        /// Compares two instances of <see cref="EventId"/> struct for equality
        /// </summary>
        /// <param name="left">Left operand</param>
        /// <param name="right">Right operand</param>
        /// <returns><c>true</c> if they are equal by id, <c>false</c> otherwise</returns>
        public static bool operator ==(EventId left, EventId right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compares two instances of <see cref="EventId"/> struct for inequality
        /// </summary>
        /// <param name="left">Left operand</param>
        /// <param name="right">Right operand</param>
        /// <returns><c>false</c> if they are equal by id, <c>true</c> otherwise</returns>
        public static bool operator !=(EventId left, EventId right)
        {
            return !left.Equals(right);
        }

        /// <summary>
        /// Converts an instance of <see cref="EventId"/> struct to its string representation
        /// </summary>
        /// <param name="arg">An instance of <see cref="EventId"/> struct</param>
        /// <returns>Returns value stored in <see cref="Value"/> property</returns>
        public static implicit operator string(EventId arg)
        {
            return arg.Value;
        }
    }
}
