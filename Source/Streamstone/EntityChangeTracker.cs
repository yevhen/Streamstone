using System;
using System.Linq;
using System.Collections.Generic;

namespace Streamstone
{
    class EntityChangeTracker
    {
        readonly Dictionary<string, EntityChangeRecord> records = 
             new Dictionary<string, EntityChangeRecord>();

        public IEnumerable<EntityOperation> Compute()
        {
            return records.Values
                          .Select(record => record.Compute())
                          .Where(operation => operation != null);
        }

        public void Record(IEnumerable<EntityOperation> operations)
        {
            foreach (var operation in operations)
                Record(operation);
        }

        void Record(EntityOperation operation)
        {
            var entityKey = operation.Entity.RowKey;

            EntityChangeRecord record; 
            if (!records.TryGetValue(entityKey, out record))
            {
                record = new EntityChangeRecord(operation);
                records.Add(entityKey, record);
                return;
            }

            record.Merge(operation);
        }

        class EntityChangeRecord
        {
            EntityOperation current;

            internal EntityChangeRecord(EntityOperation first)
            {
                current = first;
            }

            public EntityOperation Compute()
            {
                if (current == EntityOperation.None)
                    return null;

                var transient = string.IsNullOrEmpty(current.Entity.ETag);

                if (transient && current is EntityOperation.Replace)
                    throw new InvalidOperationException(
                        string.Format("'Replace' operation of entity '{0}' with RowKey '{1}' is missing Etag", 
                                      current.Entity.GetType(), current.Entity.RowKey));

                if (transient && current is EntityOperation.Delete)
                    return null;

                return current;
            }

            public void Merge(EntityOperation next)
            {
                if (current != EntityOperation.None 
                    && next.Entity != current.Entity)
                    throw new InvalidOperationException(
                        "Mistaken identity. Different instance of entity " +
                        "has been already registered in this session under RowKey: " +
                         next.Entity.RowKey);

                current = current.Merge(next);
            }
        }
    }
}