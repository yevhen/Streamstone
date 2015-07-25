using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public class Partition
    {
        static readonly string[] separator = {"|"};

        public readonly CloudTable Table;
        public readonly string PartitionKey;
        public readonly string RowKeyPrefix;
        public readonly string Key;

        public Partition(CloudTable table, string key)
        {
            Requires.NotNull(table, "table");
            Requires.NotNullOrEmpty(key, "key");

            var parts = key.Split(separator, 2, 
                StringSplitOptions.RemoveEmptyEntries);

            Table = table;
            
            PartitionKey = parts[0];
            RowKeyPrefix = parts.Length > 1 
                            ? parts[1] + separator[0] 
                            : "";
            Key = key;
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
            return string.Format("{0}.{1}", Table.Name, Key);
        }
    }
}
