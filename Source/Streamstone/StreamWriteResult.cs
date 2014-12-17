using System;
using System.Linq;

namespace Streamstone
{
    public sealed class StreamWriteResult
    {
        public readonly Stream Stream;
        public readonly StoredEvent[] Events;

        internal StreamWriteResult(Stream stream, StoredEvent[] events)
        {
            Stream = stream;
            Events = events;
        }
    }
}