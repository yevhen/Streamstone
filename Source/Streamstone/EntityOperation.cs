using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    class EntityOperation
    {
        public readonly ITableEntity Entity;
        public readonly TableOperation Operation;

        public EntityOperation(ITableEntity entity, TableOperation operation)
        {
            Entity = entity;
            Operation = operation;
        }

        internal EntityOperation Apply(Partition partition)
        {
            Entity.PartitionKey = partition.PartitionKey;
            return this;
        }
    }
}