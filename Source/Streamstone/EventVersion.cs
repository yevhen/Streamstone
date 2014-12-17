using System;
using System.Globalization;
using System.Linq;

namespace Streamstone
{
    public struct EventVersion
    {
        const int PrefixLength = 6;
        const int VersionLength = 10;
        const int ValidKeyLength = PrefixLength + VersionLength;

        readonly int version;

        public EventVersion(string key)
        {
            Requires.NotNull(key, "key");

            if (key.Length != ValidKeyLength)
                throw new ArgumentException("Valid event entity RowKey should be exactly 16 chars in length", "key");

            string number = key.Substring(EventEntity.RowKeyPrefix.Length);

            if (!int.TryParse(number, NumberStyles.None, CultureInfo.InvariantCulture, out version))
                throw new ArgumentException("Invalid event entity RowKey. Should be encoded as SS-SE-0000000001");
        }

        public static implicit operator int(EventVersion arg)    { return arg.version; }
        public static implicit operator EventVersion(string arg) { return new EventVersion(arg); }
    }
}