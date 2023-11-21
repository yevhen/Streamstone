using System;

using Azure.Data.Tables;
using Streamstone.Utility;

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

        protected abstract TableTransactionAction AsTableTransactionAction();

        public static implicit operator TableTransactionAction(EntityOperation arg)
        {
            return arg.AsTableTransactionAction();
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
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.Add, Entity.ToTableEntity(), Entity.ETag);
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
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpdateReplace, Entity.ToTableEntity(), Entity.ETag);
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
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.Delete, Entity.ToTableEntity(), Entity.ETag);
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
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpsertMerge, Entity.ToTableEntity(), Entity.ETag);
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
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpsertReplace, Entity.ToTableEntity(), Entity.ETag);
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

        internal class UpdateMerge : EntityOperation
        {
            public UpdateMerge(ITableEntity entity)
                : base(entity)
            { }

            protected override TableTransactionAction AsTableTransactionAction()
            {
                return new TableTransactionAction(TableTransactionActionType.UpdateMerge, Entity.ToTableEntity(), Entity.ETag);
            }

            public override EntityOperation Merge(EntityOperation other) => 
                throw new InvalidOperationException("Internal-only stream header merge operation");
        }

        class Null : EntityOperation
        {
            internal Null()
                : base(null)
            {}

            protected override TableTransactionAction AsTableTransactionAction()
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

                if (other is InsertOrReplace)
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