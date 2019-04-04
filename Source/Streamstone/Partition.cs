using System;
using System.Linq;

using Microsoft.Azure.Cosmos.Table;

namespace Streamstone
{
    /// <summary>
    /// Represents table partition. Virtual partitions are created 
    /// by using <c>`|`</c> split separator in a key.
    /// </summary>
    public sealed class Partition
    {
        static readonly string[] separator = {"|"};

        /// <summary>
        /// The table in which this partition resides
        /// </summary>
        public readonly CloudTable Table;

        /// <summary>
        /// The partition key
        /// </summary>
        public readonly string PartitionKey;

        /// <summary>
        /// The row key prefix. Will be non-empty only for virtual partitions
        /// </summary>
        public readonly string RowKeyPrefix;

        /// <summary>
        /// The full key of this partition
        /// </summary>
        public readonly string Key;

        /// <summary>
        /// Initializes a new instance of the <see cref="Partition"/> class.
        /// </summary>
        /// <param name="table">The cloud table.</param>
        /// <param name="key">The full key.</param>
        /// <remarks>Use "partitionkey|rowkeyprefix" key syntax to create virtual partition</remarks>
        public Partition(CloudTable table, string key)
        {
            Requires.NotNull(table, nameof(table));
            Requires.NotNullOrEmpty(key, nameof(key));

            var parts = key.Split(separator, 2, 
                StringSplitOptions.RemoveEmptyEntries);

            Table = table;
            
            PartitionKey = parts[0];
            RowKeyPrefix = parts.Length > 1 
                            ? parts[1] + separator[0] 
                            : "";
            Key = key;
        }

        /// <summary>
        /// Creates virtual partition using provided partition key and row key prefix.
        /// </summary>
        /// <param name="table">The cloud table.</param>
        /// <param name="partitionKey">The partition's own key.</param>
        /// <param name="rowKeyPrefix">The row's key prefix.</param>
        public Partition(CloudTable table, string partitionKey, string rowKeyPrefix)
        {
            Requires.NotNull(table, nameof(table));
            Requires.NotNullOrEmpty(partitionKey, nameof(partitionKey));
            Requires.NotNullOrEmpty(rowKeyPrefix, nameof(rowKeyPrefix));

            if (partitionKey.Contains(separator[0]))
                throw new ArgumentException(
                    "Partition key cannot contain virtual partition separator", "partitionKey");

            Table = table;

            PartitionKey = partitionKey;
            RowKeyPrefix = rowKeyPrefix;

            Key = string.Format("{0}{1}{2}", partitionKey, separator[0], rowKeyPrefix);
        }

        internal string StreamRowKey() => string.Format("{0}{1}", RowKeyPrefix, StreamEntity.FixedRowKey);
        internal string EventVersionRowKey(int version) => string.Format("{0}{1}{2:d10}", RowKeyPrefix, EventEntity.RowKeyPrefix, version);
        internal string EventIdRowKey(string id) => string.Format("{0}{1}{2}", RowKeyPrefix, EventIdEntity.RowKeyPrefix, id);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString() => string.Format("{0}.{1}", Table.Name, Key);
    }
}
