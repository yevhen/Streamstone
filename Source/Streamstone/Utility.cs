using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;

using Azure.Data.Tables;

namespace Streamstone
{
    namespace Utility
    {
        /// <summary>
        /// The set of helper extension methods for querying WATS tables
        /// </summary>
        public static class TableQueryExtensions
        {
            /// <summary>
            /// Query range of rows in partition having the same row key prefix
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="partition">The partition.</param>
            /// <param name="prefix">The row key prefix.</param>
            /// <returns>An instance of <see cref="IEnumerable{T}"/> that allow to scroll over all rows</returns>
            public static IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(this Partition partition, string prefix) where TEntity : class, ITableEntity, new()
            {
                var range = new PrefixRange(prefix);

                var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partition.PartitionKey} and RowKey ge {range.Start} and RowKey lt {range.End}");

                var pageable = partition.Table.Query<TableEntity>(filter);

                foreach (var entity in pageable)
                {
                    yield return entity.AsEntity<TEntity>();
                }
            }

            // /// <summary>
            // /// Applies row key prefix criteria to given table which allows to query a range of rows
            // /// </summary>
            // /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            // /// <param name="table">The table.</param>
            // /// <param name="filter">The row key prefix filter.</param>
            // /// <returns>An instance of <see cref="IEnumerable{T}"/> that allow further criteria to be added</returns>
            public static IEnumerable<TEntity> ExecuteQuery<TEntity>(this TableClient table, string filter) where TEntity : class, ITableEntity, new()
            {
                var pageable = table.Query<TEntity>(filter);

                foreach (var entity in pageable)
                {
                    yield return entity;
                }
            }
        }

        public static class TableEntityExtensions
        {
            public static TableEntity ToTableEntity(this ITableEntity entity)
            {
                var tableEntity = new TableEntity
                {
                    PartitionKey = entity.PartitionKey,
                    RowKey = entity.RowKey,
                    ETag = entity.ETag,
                    Timestamp = entity.Timestamp
                };

                foreach (var property in entity.GetType().GetTypeInfo().DeclaredProperties)
                {
                    if (property.Name
                        is nameof(ITableEntity.PartitionKey)
                        or nameof(ITableEntity.RowKey)
                        or nameof(ITableEntity.Timestamp)
                        or nameof(ITableEntity.ETag))
                        continue;

                    if (property.PropertyType.IsAssignableTo(typeof(PropertyMap)))
                    {
                        foreach (var mappedProperty in (PropertyMap)property.GetValue(entity))
                        {
                            tableEntity.Add(mappedProperty.Key, mappedProperty.Value);
                        }
                        continue;
                    }

                    if (property.GetCustomAttribute<IgnoreDataMemberAttribute>(true) != null)
                        continue;

                    tableEntity.Add(property.Name, property.GetValue(entity));
                }

                return tableEntity;
            }

            public static T AsEntity<T>(this TableEntity entity)
                where T : class, new()
            {
                if (typeof(T) == typeof(TableEntity))
                    return entity as T;

                var t = new T();
                entity.CopyTo(t);

                return t;
            }

            public static void CopyTo(this TableEntity entity, object target)
            {
                foreach (var property in target.GetType().GetTypeInfo().DeclaredProperties)
                {
                    if (property.GetCustomAttribute<IgnoreDataMemberAttribute>(true) != null)
                        continue;

                    if (property.PropertyType.IsAssignableTo(typeof(PropertyMap)))
                    {
                        var propertyMap = (PropertyMap)property.GetValue(target);
                        propertyMap.CopyFrom(entity);
                        property.SetValue(target, propertyMap);
                        continue;
                    }

                    if (property.Name == nameof(ITableEntity.ETag))
                    {
                        property.SetValue(target, entity.ETag);
                        continue;
                    }
                    
                    if (entity.TryGetValue(property.Name, out var value))
                    {
                        property.SetValue(target, value);
                        continue;
                    }

                    throw new Exception($"Failed to find value for property '{property.Name}' when copying TableEntity to {target.GetType().Name}");
                }
            }
        }

        /// <summary>
        /// Represents lexicographical range
        /// </summary>
        public struct PrefixRange
        {
            /// <summary>
            /// The start of the range
            /// </summary>
            public readonly string Start;

            /// <summary>
            /// The end of the range
            /// </summary>
            public readonly string End;

            /// <summary>
            /// Initializes a new instance of the <see cref="PrefixRange"/> struct.
            /// </summary>
            /// <param name="prefix">The prefix upon which to build a range.</param>
            public PrefixRange(string prefix)
            {
                Requires.NotNullOrEmpty(prefix, nameof(prefix));

                Start = prefix;

                var length = prefix.Length - 1;
                var lastChar = prefix[length];
                var nextLastChar = (char)(lastChar + 1);

                End = prefix.Substring(0, length) + nextLastChar;
            }
        }
    }
}