using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;

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

        public abstract Task Run();
    }
}
