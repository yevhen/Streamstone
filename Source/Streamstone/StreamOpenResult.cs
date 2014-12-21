using System;
using System.Linq;

namespace Streamstone
{
    public sealed class StreamOpenResult
    {
        internal static readonly StreamOpenResult NotFound = new StreamOpenResult(false, null);

        public readonly bool Found;
        public readonly Stream Stream;

        internal StreamOpenResult(bool found, Stream stream)
        {
            Found = found;
            Stream = stream;
        }
    }
}
