using System.Collections.Generic;

using Microsoft.Azure.Cosmos.Table;

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
            public static IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(this Partition partition, string prefix) where TEntity : ITableEntity, new()
            {
                var filter = 
                      TableQuery.CombineFilters(
                          TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition.PartitionKey),
                          TableOperators.And,
                          WhereRowKeyPrefixFilter(prefix));

                var query = new TableQuery<TEntity>().Where(filter);
                TableContinuationToken token = null;
                do
                {
                    var segment = partition.Table.ExecuteQuerySegmentedAsync(query, null).Result;
                    token = segment.ContinuationToken;
                    foreach (var res in segment.Results)
                        yield return res;
                }
                while (token != null);               
            }

            /// <summary>
            /// Applies row key prefix criteria to given table which allows to query a range of rows
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="table">The table.</param>
            /// <param name="filter">The row key prefix filter.</param>
            /// <returns>An instance of <see cref="IEnumerable{T}"/> that alllow further criterias to be added</returns>
            public static IEnumerable<TEntity> ExecuteQuery<TEntity>(this CloudTable table, string filter) where TEntity : ITableEntity, new()
            {
                var query = new TableQuery<TEntity>().Where(filter);
                TableContinuationToken token;
                do
                {
                    var segment = table.ExecuteQuerySegmentedAsync(query, null).Result;
                    token = segment.ContinuationToken;
                    foreach (var res in segment.Results)
                        yield return res;
                }
                while (token != null);
            }

            static string WhereRowKeyPrefixFilter(string prefix)
            {
                var range = new PrefixRange(prefix);
                var filter =
                     TableQuery.CombineFilters(
                         TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, range.Start),
                         TableOperators.And,
                         TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, range.End));

                return filter;
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