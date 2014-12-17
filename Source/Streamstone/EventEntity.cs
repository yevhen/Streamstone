using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class EventEntity : TableEntity
    {
        public const string RowKeyPrefix = "SS-SE-";

        public EventEntity()
        {
            Properties = EventProperties.None;
        }

        internal EventEntity(string partition, string id, int version, EventProperties properties)
        {
            Id = id;
            PartitionKey = partition;
            RowKey = new EventKey(version);
            Properties = properties;
            Version = version;   
        }

        public string Id                    { get; set; }
        public int Version                  { get; set; }
        public EventProperties Properties   { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            Properties = EventProperties.ReadEntity(properties);
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var result = base.WriteEntity(operationContext);
            Properties.WriteTo(result);
            return result;
        }
    }
}