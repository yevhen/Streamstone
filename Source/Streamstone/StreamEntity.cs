using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class StreamEntity : TableEntity
    {
        internal const string FixedRowKey = "SS-HEAD";

        public StreamEntity()
        {
            Properties = StreamProperties.None;
        }

        internal StreamEntity(string partition, StreamProperties properties, string etag, int start, int count, int version)
        {
            PartitionKey = partition;
            RowKey = FixedRowKey;
            Properties = properties;
            ETag = etag;
            Start = start;
            Count = count;
            Version = version;
        }

        public int Start                    { get; set; }
        public int Count                    { get; set; }
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

        internal static StreamEntity From(DynamicTableEntity entity)
        {
            return new StreamEntity
            {
                PartitionKey = entity.PartitionKey,
                RowKey = entity.RowKey,
                ETag = entity.ETag,
                Timestamp = entity.Timestamp,
                Start = (int)entity["Start"].PropertyAsObject,
                Count = (int)entity["Count"].PropertyAsObject,
                Version = (int)entity["Version"].PropertyAsObject,
                Properties = StreamProperties.From(entity)
            };
        }
    }
}