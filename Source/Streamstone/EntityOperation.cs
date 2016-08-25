using System;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    abstract class EntityOperation
    {
        public static readonly EntityOperation None = new Null();

        public readonly ITableEntity Entity;

        EntityOperation(ITableEntity entity)
        {
            Entity = entity;
        }

        protected abstract TableOperation AsTableOperation();

        public static implicit operator TableOperation(EntityOperation arg)
        {
            return arg.AsTableOperation();
        }

        public abstract EntityOperation Merge(EntityOperation other);

        Exception InvalidMerge(EntityOperation other)
        {
            var message = string.Format("Included '{0}' operation cannot be followed by '{1}' operation",
                GetType().Name, other.GetType().Name);

            return new InvalidOperationException(message);
        }

        public EntityOperation Apply(Partition partition)
        {
            Entity.PartitionKey = partition.PartitionKey;
            return this;
        }

        public class Insert : EntityOperation
        {
            public Insert(ITableEntity entity)
                : base(entity)
            {
            }

            protected override TableOperation AsTableOperation()
            {
                return TableOperation.Insert(Entity);
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    return new Insert(other.Entity);

                if (other is Delete)
                    return None;

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class Replace : EntityOperation
        {
            public Replace(ITableEntity entity)
                : base(entity)
            {
            }

            protected override TableOperation AsTableOperation()
            {
                return TableOperation.Replace(Entity);
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    return other;

                if (other is Delete)
                    return other;

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class Delete : EntityOperation
        {
            public Delete(ITableEntity entity)
                : base(entity)
            {
            }

            protected override TableOperation AsTableOperation()
            {
                return TableOperation.Delete(Entity);
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    return new Replace(other.Entity);

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class InsertOrMerge : EntityOperation
        {
            public InsertOrMerge(ITableEntity entity)
                : base(entity)
            {
            }

            protected override TableOperation AsTableOperation()
            {
                return TableOperation.InsertOrMerge(Entity);
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    return other;

                if (other is InsertOrReplace)
                    throw InvalidMerge(other);

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        public class InsertOrReplace : EntityOperation
        {
            public InsertOrReplace(ITableEntity entity)
                : base(entity)
            {
            }

            protected override TableOperation AsTableOperation()
            {
                return TableOperation.InsertOrReplace(Entity);
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    throw InvalidMerge(other);

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    throw InvalidMerge(other);

                if (other is InsertOrReplace)
                    return other;

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }
        }

        class Null : EntityOperation
        {
            internal Null()
                : base(null)
            {
            }

            protected override TableOperation AsTableOperation()
            {
                throw new NotImplementedException();
            }

            public override EntityOperation Merge(EntityOperation other)
            {
                if (other is Insert)
                    return other;

                if (other is Replace)
                    throw InvalidMerge(other);

                if (other is Delete)
                    throw InvalidMerge(other);

                if (other is InsertOrMerge)
                    return other;

                throw new InvalidOperationException("Unsupported operation type: " + other.GetType());
            }

            new static Exception InvalidMerge(EntityOperation other)
            {
                var message = string.Format("Included 'Delete' operation interdifused with " +
                                            "preceding 'Insert' operation. " +
                                            "'{0}' cannot be applied to NULL",
                    other.GetType());

                return new InvalidOperationException(message);
            }
        }
    }
}