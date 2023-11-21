using System.Linq;

using Azure.Data.Tables;

namespace Streamstone
{
    /// <summary>
    /// Represent the result of stream write operation.
    /// </summary>
    public sealed class StreamWriteResult
    {
        /// <summary>
        /// The updated stream header, that could/should be used for subsequent operations on a stream
        /// </summary>
        public readonly Stream Stream;

        /// <summary>
        /// The events that were written
        /// </summary>
        public readonly RecordedEvent[] Events;

        /// <summary>
        /// The additional entities that were written in this batch
        /// </summary>
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