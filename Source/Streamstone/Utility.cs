// ReSharper disable StringCompareToIsCultureSpecific

using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;

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
            /// <returns>An instance of <see cref="IQueryable"/> that alllow further criterias to be added</returns>
            public static TableQuery<TEntity> RowKeyPrefixQuery<TEntity>(this Partition partition, string prefix) where TEntity : ITableEntity, new()
            {
                var filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal,
                    partition.PartitionKey);
                var query = new TableQuery<TEntity>().Where(filter);
                return query.WhereRowKeyPrefix(prefix);
            }

            /// <summary>
            /// Applies row key prefix criteria to given queryable which allows to query a range of rows
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="queryable">The queryable.</param>
            /// <param name="prefix">The row key prefix.</param>
            /// <returns>An instance of <see cref="IQueryable"/> that alllow further criterias to be added</returns>
            public static TableQuery<TEntity> WhereRowKeyPrefix<TEntity>(this TableQuery<TEntity> queryable, string prefix) where TEntity : ITableEntity, new()
            {
                var range = new PrefixRange(prefix);

                var rowKeyStartFilter = TableQuery.GenerateFilterCondition("RowKey",
                    QueryComparisons.GreaterThanOrEqual, range.Start);

                var rowKeyEndFilter = TableQuery.GenerateFilterCondition("RowKey",
                    QueryComparisons.LessThan, range.End);

                var rowKeyBetweenFilter = TableQuery.CombineFilters(rowKeyStartFilter, TableOperators.Or, rowKeyEndFilter);

                return queryable.Where(rowKeyBetweenFilter);
            }

            /// <summary>
            /// Gets all the entities for a given <see cref="TableQuery{TEntity}">query</see>.
            /// </summary>
            /// <typeparam name="TEntity">The entity type to return</typeparam>
            /// <param name="table">The table on which to run the query</param>
            /// <param name="query">The query</param>
            /// <returns>All the entities in a <paramref name="table"/> for a given
            /// <see cref="TableQuery{TEntity}">query</see>.</returns>
            public static List<TEntity> RetrieveAll<TEntity>(this CloudTable table, TableQuery<TEntity> query)
                where TEntity : ITableEntity, new()
            {
                var entities = new List<TEntity>();
                TableContinuationToken token = null;

                do
                {
                    var page = query.Take(512);

                    var segment = table.ExecuteQuerySegmentedAsync(page, token).Result;
                    token = segment.ContinuationToken;

                    entities.AddRange(segment.Results);
                }
                while (token != null);

                return entities;
            }

            /// <summary>
            /// Gets all entity for a partition key/row key combination.
            /// </summary>
            /// <typeparam name="TEntity">The entity type to return</typeparam>
            /// <param name="partition">The partition from which to run the query</param>
            /// <param name="rowKey">The row's key in the partition</param>
            /// <returns>The entity corresponding to the partition key/row key combination.</returns>
            public static TEntity RetrieveEntity<TEntity>(this Partition partition, string rowKey)
                where TEntity : TableEntity, new()
            {
                var partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey",
                    QueryComparisons.Equal, partition.PartitionKey);

                var rowKeyFilter = TableQuery.GenerateFilterCondition("RowKey",
                    QueryComparisons.Equal, rowKey);

                var filter = TableQuery.CombineFilters(
                    partitionKeyFilter, TableOperators.And, rowKeyFilter);

                TableQuery<TEntity> query = new TableQuery<TEntity>().Where(filter);

                return partition.Table.RetrieveAll(query).SingleOrDefault();
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
                Requires.NotNullOrEmpty(prefix, "prefix");
                
                Start = prefix;

                var length = prefix.Length - 1;
                var lastChar = prefix[length];
                var nextLastChar = (char)(lastChar + 1);

                End = prefix.Substring(0, length) + nextLastChar;
            }
        }
    }
}