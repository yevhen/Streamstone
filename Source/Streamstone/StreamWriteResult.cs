using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public sealed class StreamWriteResult
    {
        public readonly Stream Stream;
        public readonly RecordedEvent[] Events;
        public readonly ITableEntity[] Includes;

        internal StreamWriteResult(Stream stream, RecordedEvent[] events)
        {
            Stream = stream;
            Events = events;
            Includes = events
                .SelectMany(x => x.IncludedOperations.Select(y => y.Entity))
                .ToArray();
        }
    }
}