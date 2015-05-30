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

        Include(ITableEntity entity, IncludeType type, TableOperation operation)
        {
            Entity = entity;
            Type = type;

            this.operation = operation;
        }

        internal TableOperation Apply(Partition partition)
        {
            Entity.PartitionKey = partition.PartitionKey;
            return operation;
        }

        public static Include Delete(ITableEntity entity)
        {
            return new Include(entity, IncludeType.Delete, TableOperation.Delete(entity));
        }
        
        public static Include Insert(ITableEntity entity)
        {
            return new Include(entity, IncludeType.Insert, TableOperation.Insert(entity));
        }

        public static Include InsertOrMerge(ITableEntity entity)
        {
            return new Include(entity, IncludeType.InsertOrMerge, TableOperation.InsertOrMerge(entity));
        }

        public static Include InsertOrReplace(ITableEntity entity)
        {
            return new Include(entity, IncludeType.InsertOrReplace, TableOperation.InsertOrReplace(entity));
        }
    }
}
