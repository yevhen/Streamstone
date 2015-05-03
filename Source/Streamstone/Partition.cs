using System;
using System.Diagnostics;
using System.Linq;

namespace Streamstone
{
    [DebuggerDisplay("{ToString()}")]
    public class Partition
    {
        static readonly string[] separator = {"|"};

        readonly string partitionKey;
        readonly string rowKeyPrefix;

        public Partition(string key)
        {
            Requires.NotNullOrEmpty(key, "key");

            var parts = key.Split(separator, 2, 
                StringSplitOptions.RemoveEmptyEntries);

            partitionKey = parts[0];
            rowKeyPrefix = parts.Length > 1 
                            ? parts[1] + separator[0] 
                            : "";
        }

        public string PartitionKey
        {
            get { return partitionKey; }
        }

        public string StreamRowKey()
        {
            return string.Format("{0}{1}", rowKeyPrefix, StreamEntity.FixedRowKey);
        }

        public string EventVersionRowKey(int version)
        {
            return string.Format("{0}{1}{2:d10}", rowKeyPrefix, EventEntity.RowKeyPrefix, version);
        }

        public string EventIdRowKey(string id)
        {
            return string.Format("{0}{1}{2}", rowKeyPrefix, EventIdEntity.RowKeyPrefix, id);
        }

        public override string ToString()
        {
            return string.Format("{0}{1}", partitionKey, rowKeyPrefix);
        }

        public static implicit operator Partition(string arg)
        {
            return new Partition(arg);
        }
    }
}
