using System;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represents the result of a single stream read operation.
    /// </summary>
    /// <typeparam name="T">The type of entity this slice will return</typeparam>
    public sealed class StreamSlice<T> where T : class, new()
    {
        static readonly T[] Empty = new T[0];

        /// <summary>
        /// The stream header which has been read
        /// </summary>
        public readonly Stream Stream;

        /// <summary>
        /// The events that has been read (page)
        /// </summary>
        public readonly T[] Events = Empty;

        /// <summary>
        /// The next event number that can be read.
        /// </summary>
        public readonly int NextEventNumber;

        /// <summary>
        ///  A boolean flag representing whether or not this is the end of the stream.
        /// </summary>
        public readonly bool IsEndOfStream;

        internal StreamSlice(Stream stream, T[] events, int startVersion, int sliceSize)
        {
            const int endOfStream = -1;

            Stream = stream;
            Events = events;

            IsEndOfStream = (startVersion + sliceSize - 1) >= stream.Version;
            NextEventNumber = IsEndOfStream ? endOfStream : startVersion + sliceSize;
        }
    }
}