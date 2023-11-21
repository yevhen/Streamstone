using System;

using Azure;
using Azure.Data.Tables;

namespace Streamstone
{
    class TestStreamEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public DateTimeOffset Created { get; set; }

        public bool Active { get; set; }
    }

    class TestEventEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public string Id { get; set; }

        public string Type { get; set; }

        public string Data { get; set; }
    }

    class TestRecordedEventEntity : TestEventEntity
    {
        public long Version { get; set; }
    }
}