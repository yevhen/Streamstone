using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represents the set of additional event includes
    /// </summary>
    public sealed class EventIncludes : IEnumerable<Include>
    {
        /// <summary>
        /// An empty set of event includes
        /// </summary>
        public static readonly EventIncludes None = new EventIncludes(new Include[0]);

        readonly Include[] includes;

        EventIncludes(Include[] includes)
        {
            this.includes = includes;
        }

        /// <summary>
        /// Creates new instance of <see cref="EventIncludes"/> class using given array of <see cref="Include"/> operations
        /// </summary>
        /// <param name="includes">An array of <see cref="Include"/> class instances</param>
        /// <returns>New instance of <see cref="EventIncludes"/> class</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="includes"/> is <c>null</c></exception>
        public static EventIncludes From(params Include[] includes)
        {
            Requires.NotNull(includes, nameof(includes));
            return new EventIncludes(includes);
        }

        /// <summary>
        /// Creates new instance of <see cref="EventIncludes"/> class using given enumerable of <see cref="Include"/> operations
        /// </summary>
        /// <param name="includes">An enumerable of <see cref="Include"/> class instances</param>
        /// <returns>New instance of <see cref="EventIncludes"/> class</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="includes"/> is <c>null</c></exception>
        public static EventIncludes From(IEnumerable<Include> includes)
        {
            // ReSharper disable PossibleMultipleEnumeration
            Requires.NotNull(includes, nameof(includes));
            return new EventIncludes(includes.ToArray());
            // ReSharper restore PossibleMultipleEnumeration
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<Include> GetEnumerator()
        {
            return ((IEnumerable<Include>)includes).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return includes.GetEnumerator();
        }
    }
}
