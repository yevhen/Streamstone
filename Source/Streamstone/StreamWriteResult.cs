using System;
using System.Linq;

namespace Streamstone
{
    public sealed class StreamWriteResult
    {
        public readonly Stream Stream;
        public readonly RecordedEvent[] Events;

        internal StreamWriteResult(Stream stream, RecordedEvent[] events)
        {
            Stream = stream;
            Events = events;
        }
    }
}