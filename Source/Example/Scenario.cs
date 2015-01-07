using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Example
{
    public abstract class Scenario
    {
        protected CloudTable Table;
        protected string Partition;

        public void Initialize(CloudTable table, int partition)
        {
            Table = table;
            Partition = partition.ToString();
        }

        public abstract void Run();
    }
}
