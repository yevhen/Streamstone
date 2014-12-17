using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class TestEventEntity : TableEntity
    {
        public string Id   { get; set; }
        public string Type { get; set; }
        public string Data { get; set; }
    }

    class TestStoredEventEntity : TestEventEntity
    {
        public int Version { get; set; }
    }
}
