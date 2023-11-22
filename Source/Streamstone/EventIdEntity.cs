using Azure.Data.Tables;

namespace Streamstone
{
    sealed class EventIdEntity : DynamicTableEntity
    {
        public const string RowKeyPrefix = "SS-UID-";

        public EventIdEntity()
        {
            Version = 0;
        }

        public EventIdEntity(Partition partition, RecordedEvent @event)
        {
            Event = @event;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventIdRowKey(@event.Id);
            Version = @event.Version;
        }

        public int Version
        {
            get => (int)this[nameof(Version)];
            set => this[nameof(Version)] = value;
        }

        public RecordedEvent Event { get; set; }

        public static EventIdEntity From(TableEntity entity)
        {
            return new EventIdEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                Timestamp = entity.Timestamp,
                ETag = entity.ETag,
                Version = (int)entity.GetInt32(nameof(Version)),
            };
        }
    }
}