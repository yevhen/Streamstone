using System;

namespace Streamstone
{
    /// <summary>
    /// Represents unique id of the event
    /// </summary>
    public readonly record struct EventId
    {
        /// <summary>
        /// Value for event which has no unique id
        /// </summary>
        public static readonly EventId None = new();

        /// <summary>
        /// Value of unique id
        /// </summary>
        public string Value { get; init; }

        /// <summary>
        /// Creates instance of  <see cref="EventId"/> from <see cref="string"/> id
        /// </summary>
        /// <param name="id">Value of new <see cref="EventId"/></param>
        /// <returns>New instance of <see cref="EventEntity"/> class</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="id"/> is <c>null</c></exception>
        public static EventId From(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return new(id);
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
        public static EventId From(Guid id, string format = "D") => new(id.ToString(format));

        private EventId(string value) => Value = value;

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>
        /// A 32-bit signed integer that is the hash code for this instance.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode() => Value?.GetHashCode() ?? 0;

        /// <summary>
        /// Converts an instance of <see cref="EventId"/> struct to its string representation
        /// </summary>
        /// <param name="arg">An instance of <see cref="EventId"/> struct</param>
        /// <returns>Returns value stored in <see cref="Value"/> property</returns>
        public static implicit operator string(EventId arg) => arg.Value;
    }
}