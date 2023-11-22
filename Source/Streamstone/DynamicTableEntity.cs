using System;
using System.Collections.Generic;

using Azure;
using Azure.Data.Tables;

namespace Streamstone
{
    class DynamicTableEntity : Dictionary<string, object>
    {
        const string EtagOdata = "odata.etag";

        public string PartitionKey
        {
            get => (string)this[nameof(PartitionKey)];
            set => this[nameof(PartitionKey)] = value;
        }

        public string RowKey
        {
            get => (string)this[nameof(RowKey)];
            set => this[nameof(RowKey)] = value;
        }

        public DateTimeOffset? Timestamp
        {
            get => TryGetValue(nameof(Timestamp), out var value) ? (DateTimeOffset?)value : null;
            set => this[nameof(Timestamp)] = value;
        }

        public ETag ETag
        {
            get => TryGetValue(EtagOdata, out var value) ? (ETag)value : default;
            set => this[EtagOdata] = value;
        }

        public virtual TableEntity ToTableEntity()
        {
            return new TableEntity(this)
            {
                PartitionKey = PartitionKey,
                RowKey = RowKey,
                Timestamp = Timestamp,
                ETag = ETag
            };
        }
    }
}