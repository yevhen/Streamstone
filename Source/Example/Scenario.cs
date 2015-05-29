using System;
using System.Linq;

using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example
{
    public abstract class Scenario
    {
        protected string Id;
        protected CloudTable Table;
        protected Partition Partition;

        public void Initialize(CloudTable table, string id)
        {
            Id = id;
            Table = table;
            Partition = new Partition(table, id);
        }

        public abstract void Run();
    }
}
