using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public enum IncludeType
    {
        Insert,
        InsertOrReplace,
        InsertOrMerge,
        Delete
    }

    public sealed class Include
    {
        public readonly ITableEntity Entity;
        public readonly IncludeType Type;
        readonly TableOperation operation;

        Include(IncludeType type, ITableEntity entity, TableOperation operation)
        {
            Entity = entity;
            Type = type;
            this.operation = operation;            
        }

        internal TableOperation Apply(Partition partition, int version)
        {
            Entity.PartitionKey = partition.PartitionKey;

            var versioned = Entity as IVersionedEntity;
            if (versioned != null)
                versioned.Version = version;

            return operation;
        }

        public static Include Delete(ITableEntity entity)
        {
            return new Include(IncludeType.Delete, entity, TableOperation.Delete(entity));
        }
        
        public static Include Insert(ITableEntity entity)
        {
            return new Include(IncludeType.Insert, entity, TableOperation.Insert(entity));
        }
        
        public static Include InsertOrMerge(ITableEntity entity)
        {
            return new Include(IncludeType.InsertOrMerge, entity, TableOperation.InsertOrMerge(entity));
        }
        
        public static Include InsertOrReplace(ITableEntity entity)
        {
            return new Include(IncludeType.InsertOrReplace, entity, TableOperation.InsertOrReplace(entity));
        }
    }
}
