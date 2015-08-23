using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class StreamEntity : TableEntity
    {
        public const string FixedRowKey = "SS-HEAD";

        public StreamEntity()
        {
            Properties = StreamProperties.None;
        }

        public StreamEntity(Partition partition, string etag, int version, StreamProperties properties)
        {
            Partition = partition;
            PartitionKey = partition.PartitionKey;
            RowKey = partition.StreamRowKey();
            ETag = etag;
            Version = version;
            Properties = properties;
        }

        public int Version                  { get; set; }
        public StreamProperties Properties  { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            Properties = StreamProperties.ReadEntity(properties);
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var result = base.WriteEntity(operationContext);
            Properties.WriteTo(result);
            return result;
        }

        public static StreamEntity From(DynamicTableEntity entity)
        {
            return new StreamEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Version = (int)entity["Version"].PropertyAsObject,
                Properties = StreamProperties.From(entity)
            };
        }

        public EntityOperation Operation()
        {
            var isTransient = ETag == null;

            return isTransient
                    ? (EntityOperation) 
                      new EntityOperation.Insert(this)
                    : new EntityOperation.Replace(this);
        }

        [IgnoreProperty]
        public Partition Partition
        {
            get; set;
        }
    }
}