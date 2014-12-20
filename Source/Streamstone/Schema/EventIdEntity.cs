using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Schema
{
    class EventIdEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {}

        internal EventIdEntity(string partition, string id, int version)
        {
            PartitionKey = partition;
            RowKey = RowKeyPrefix + id;
            Version = version;
        }

        public int Version { get; set; }
    }
}