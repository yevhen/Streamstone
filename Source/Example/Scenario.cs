using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Streamstone;

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

        public abstract Task RunAsync();
    }
}