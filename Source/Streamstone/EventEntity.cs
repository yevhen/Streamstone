using System;

using Azure;
using Azure.Data.Tables;

namespace Streamstone
{
    class EventEntity : ITableEntity
    {
        public const string RowKeyPrefix = "SS-SE-";

        public EventEntity()
        {
            Properties = EventProperties.None;
        }

        public EventEntity(Partition partition, RecordedEvent @event)
        {
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventVersionRowKey(@event.Version);
            Properties = @event.Properties;
            Version = @event.Version;   
        }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public long Version { get; set; }

        public EventProperties Properties { get; set; }
    }
}