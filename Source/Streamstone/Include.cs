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
        readonly EntityOperation operation;

        Include(ITableEntity entity, IncludeType type, TableOperation operation)
        {
            Type = type;
            this.operation = new EntityOperation(entity, operation);
        }

        public IncludeType Type
        {
            get; private set;
        }

        public ITableEntity Entity
        {
            get { return operation.Entity; }
        }

        internal EntityOperation Apply(Partition partition)
        {
            return operation.Apply(partition);
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
