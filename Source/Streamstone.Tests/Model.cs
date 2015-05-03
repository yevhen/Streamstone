using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class TestStreamEntity : TableEntity
    {
        public DateTimeOffset Created   { get; set; }
        public bool Active              { get; set; }
    }

    class TestEventEntity : TableEntity
    {
        public string Id   { get; set; }
        public string Type { get; set; }
        public string Data { get; set; }
    }

    class TestRecordedEventEntity : TestEventEntity
    {
        public int Version { get; set; }
    }
}
