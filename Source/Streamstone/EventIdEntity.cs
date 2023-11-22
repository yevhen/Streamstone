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

        public long Version
        {
            get => (long)this[nameof(Version)];
            set => this[nameof(Version)] = value;
        }

        public RecordedEvent Event { get; set; }

        public static EventIdEntity From(TableEntity entity)
        {
            return new EventIdEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Version = (long)entity.GetInt64(nameof(Version))!,
            };
        }
    }
}