using System;
using System.Linq;

namespace Streamstone
{
    public sealed class StreamSlice<T> where T : class, new()
    {
        const int EndOfStream = -1;
        static readonly T[] Empty = new T[0];

        public readonly Stream Stream;
        public readonly T[] Events = Empty;
        public readonly int NextEventNumber;
        public readonly bool IsEndOfStream;

        internal StreamSlice(Stream stream, T[] events, int startVersion, int sliceSize)
        {
            Stream = stream;
            Events = events;

            IsEndOfStream = (startVersion + sliceSize - 1) >= stream.Version;
            NextEventNumber = IsEndOfStream ? EndOfStream : startVersion + sliceSize;
        }
    }
}