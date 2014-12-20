using System;
using System.Linq;

namespace Streamstone
{
    struct EventKey
    {
        readonly string key;

        public EventKey(int version)
        {
            Requires.GreaterThanOrEqualToOne(version, "version");
            key = string.Format("{0}{1:d10}", EventEntity.RowKeyPrefix, version);
        }

        public static implicit operator string(EventKey arg) { return arg.key; }
        public static implicit operator EventKey(int arg)    { return new EventKey(arg); }
    }
}