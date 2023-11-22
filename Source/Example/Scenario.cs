using System.Threading.Tasks;

using Azure.Data.Tables;

using Streamstone;

namespace Example
{
    public abstract class Scenario
    {
        protected string Id;
        protected TableClient Table;
        protected Partition Partition;

        public void Initialize(TableClient table, string id)
        {
            Id = id;
            Table = table;
            Partition = new Partition(table, id);
        }

        public abstract Task RunAsync();
    }
}