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
            /// <returns>An instance of <see cref="IQueryable"/> that allow further criterias to be added</returns>
            public static IEnumerable<TEntity> RowKeyPrefixQuery<TEntity>(this Partition partition, string prefix) where TEntity : ITableEntity, new()
            {
                var filter = 
                      TableQuery.CombineFilters(
                          //x.PartitionKey == partition.PartitionKey
                          TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition.PartitionKey),
                          TableOperators.And,
                          //x.RowKey == Api.StreamRowKey
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
                //return table.CreateQuery<TEntity>()
                //            .Where(x => x.PartitionKey == partition.PartitionKey)
                //            .WhereRowKeyPrefix(prefix);
            }

            /// <summary>
            /// Applies row key prefix criteria to given queryable which allows to query a range of rows
            /// </summary>
            /// <typeparam name="TEntity">The type of the entity to return.</typeparam>
            /// <param name="queryable">The queryable.</param>
            /// <param name="prefix">The row key prefix.</param>
            /// <returns>An instance of <see cref="IQueryable"/> that alllow further criterias to be added</returns>
            //public static IQueryable<TEntity> WhereRowKeyPrefix<TEntity>(this IQueryable<TEntity> queryable, string prefix) where TEntity : ITableEntity, new()
            //{
            //    var range = new PrefixRange(prefix);

            //    return queryable.Where(x =>
            //                x.RowKey.CompareTo(range.Start) >= 0
            //                && x.RowKey.CompareTo(range.End) < 0);
            //}
            public static IEnumerable<TEntity> ExecuteQuery<TEntity>(this CloudTable table, string filter) where TEntity : ITableEntity, new()
            {
                var query = new TableQuery<TEntity>().Where(filter);
                TableContinuationToken token = null;
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
                         //x.RowKey.CompareTo(range.Start) >= 0
                         TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, range.Start),
                         TableOperators.And,
                         //x.RowKey.CompareTo(range.End) < 0
                         TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, range.End));

                //return queryable.Where(x =>
                //            x.RowKey.CompareTo(range.Start) >= 0
                //            && x.RowKey.CompareTo(range.End) < 0);
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