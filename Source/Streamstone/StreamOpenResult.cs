using System;
using System.Linq;

namespace Streamstone
{
    public sealed class StreamOpenResult
    {
        internal static readonly StreamOpenResult NotFound = new StreamOpenResult(false, null);

        public readonly bool Success;
        public readonly Stream Stream;

        internal StreamOpenResult(bool success, Stream stream)
        {
            Success = success;
            Stream = stream;
        }
    }
}
