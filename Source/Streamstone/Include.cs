using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    public enum IncludeType
    {
        Insert,
        Replace,
        Delete
    }

    public sealed class Include
    {
        public static Include Insert(ITableEntity entity)
        {
            return new Include(IncludeType.Insert, new EntityOperation.Insert(entity));
        }

        public static Include Replace(ITableEntity entity)
        {
            return new Include(IncludeType.Replace, new EntityOperation.Replace(entity));
        }

        public static Include Delete(ITableEntity entity)
        {
            return new Include(IncludeType.Delete, new EntityOperation.Delete(entity));
        }
 
        Include(IncludeType type, EntityOperation operation)
        {
            Type = type;
            Operation = operation;
        }

        public IncludeType Type
        {
            get; private set;
        }

        public ITableEntity Entity
        {
            get { return Operation.Entity; }
        }

        internal EntityOperation Operation
        {
            get; private set;
        }
    }
}