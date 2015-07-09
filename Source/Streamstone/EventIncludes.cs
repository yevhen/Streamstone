using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Streamstone
{
    public sealed class EventIncludes : IEnumerable<Include>
    {
        public static readonly EventIncludes None = new EventIncludes(new Include[0]);

        readonly Include[] includes;

        EventIncludes(Include[] includes)
        {
            this.includes = includes;
        }

        public static EventIncludes From(params Include[] includes)
        {
            Requires.NotNull(includes, "includes");
            return new EventIncludes(includes);
        }

        public static EventIncludes From(IEnumerable<Include> includes)
        {
            // ReSharper disable PossibleMultipleEnumeration
            Requires.NotNull(includes, "includes");
            return new EventIncludes(includes.ToArray());
            // ReSharper restore PossibleMultipleEnumeration
        }

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
