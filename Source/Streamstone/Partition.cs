using System;
using System.Diagnostics;
using System.Linq;

namespace Streamstone
{
    [DebuggerDisplay("{ToString()}")]
    public class Partition
    {
        static readonly string[] separator = {"|"};

        public readonly string PartitionKey;
        public readonly string RowKeyPrefix;

        public Partition(string key)
        {
            Requires.NotNullOrEmpty(key, "key");

            var parts = key.Split(separator, 2, 
                StringSplitOptions.RemoveEmptyEntries);

            PartitionKey = parts[0];
            RowKeyPrefix = parts.Length > 1 
                            ? parts[1] + separator[0] 
                            : "";
        }

        public string StreamRowKey()
        {
            return string.Format("{0}{1}", RowKeyPrefix, StreamEntity.FixedRowKey);
        }

        public string EventVersionRowKey(int version)
        {
            return string.Format("{0}{1}{2:d10}", RowKeyPrefix, EventEntity.RowKeyPrefix, version);
        }

        public string EventIdRowKey(string id)
        {
            return string.Format("{0}{1}{2}", RowKeyPrefix, EventIdEntity.RowKeyPrefix, id);
        }

        public override string ToString()
        {
            return string.Format("{0}{1}", PartitionKey, RowKeyPrefix);
        }

        public static implicit operator Partition(string arg)
        {
            return new Partition(arg);
        }
    }
}
