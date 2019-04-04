using System;
using System.Linq;

using Microsoft.Azure.Cosmos.Table;

namespace Streamstone
{
    class EventIdEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {}

        public EventIdEntity(Partition partition, RecordedEvent @event)
        {
            Event = @event;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventIdRowKey(@event.Id);
            Version = @event.Version;
        }

        public int Version
        {
            get; set;
        }

        [IgnoreProperty]
        public RecordedEvent Event
        {
            get; set;
        }
    }
}