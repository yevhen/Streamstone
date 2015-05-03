using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class EventIdEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {}

        public EventIdEntity(Partition partition, string id, int version)
        {
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventIdRowKey(id);
            Version = version;
        }

        public int Version { get; set; }
    }
}