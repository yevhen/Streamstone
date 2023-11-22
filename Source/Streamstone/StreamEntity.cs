using Azure;
using Azure.Data.Tables;

namespace Streamstone
{
    sealed class StreamEntity : DynamicTableEntity
    {
        public const string FixedRowKey = "SS-HEAD";

        public StreamEntity()
        {
            Properties = StreamProperties.None;
            Version = 0;
        }

        public StreamEntity(Partition partition, ETag etag, long version, StreamProperties properties)
        {
            Partition = partition;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.StreamRowKey();
            ETag = etag;
            Version = version;
            Properties = properties;
        }

        public long Version
        {
            get => (long)this[nameof(Version)];
            set => this[nameof(Version)] = value;
        }

        public StreamProperties Properties { get; set; }

        public Partition Partition { get; set; }

        public override TableEntity ToTableEntity()
        {
            var entity = base.ToTableEntity();

            foreach (var property in Properties)
                entity.Add(property.Key, property.Value);

            return entity;
        }

        public static StreamEntity From(TableEntity entity)
        {
            return new StreamEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Version = (long)entity.GetInt64(nameof(Version))!,
                Properties = StreamProperties.From(entity)
            };
        }

        public EntityOperation Operation()
        {
            var isTransient = string.IsNullOrEmpty(ETag.ToString());
            var entity = ToTableEntity();

            return isTransient ? Insert() : ReplaceOrMerge();

            EntityOperation.Insert Insert() => new EntityOperation.Insert(entity);

            EntityOperation ReplaceOrMerge() => ReferenceEquals(Properties, StreamProperties.None)
                ? new EntityOperation.UpdateMerge(entity)
                : new EntityOperation.Replace(entity);
        }
    }
}