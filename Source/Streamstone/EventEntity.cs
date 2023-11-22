using Azure.Data.Tables;

namespace Streamstone
{
    sealed class EventEntity : DynamicTableEntity
    {
        public const string RowKeyPrefix = "SS-SE-";

        public EventEntity()
        {
            Properties = EventProperties.None;
            Version = 0;
        }

        public EventEntity(Partition partition, RecordedEvent @event)
        {
            PartitionKey = partition.PartitionKey;
            RowKey = partition.EventVersionRowKey(@event.Version);
            Properties = @event.Properties;
            Version = @event.Version;
        }

        public long Version
        {
            get => (long)this[nameof(Version)];
            set => this[nameof(Version)] = value;
        }

        public EventProperties Properties { get; set; }

        public override TableEntity ToTableEntity()
        {
            var entity = base.ToTableEntity();

            foreach (var property in Properties)
                entity.Add(property.Key, property.Value);

            return entity;
        }

        public static EventEntity From(TableEntity entity)
        {
            return new EventEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Version = (long)entity.GetInt64(nameof(Version))!,
                Properties = EventProperties.From(entity)
            };
        }
    }
}