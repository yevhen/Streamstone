using System;
using System.Linq;

namespace Streamstone
{
    /// <summary>
    /// Represent the result of stream open attempt.
    /// </summary>
    public sealed class StreamOpenResult
    {
        internal static readonly StreamOpenResult NotFound = new StreamOpenResult(false, null);

        /// <summary>
        ///  A boolean flag representing whether the stream is exists (was found)
        /// </summary>
        public readonly bool Found;

        /// <summary>
        /// The stream header or <c>null</c> if stream has not been found in storage
        /// </summary>
        public readonly Stream Stream;

        internal StreamOpenResult(bool found, Stream stream)
        {
            Found = found;
            Stream = stream;
        }
    }
}
