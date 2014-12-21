using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using ExpectedObjects;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    static class StorageModel
    {
        const string TableName = "Streams";

        public static CloudTable SetUp()
        {
            var account = TestStorageAccount();

            return account == CloudStorageAccount.DevelopmentStorageAccount 
                    ? SetUpDevelopmentStorageTable(account) 
                    : SetUpAzureStorageTable(account);
        }

        static CloudTable SetUpDevelopmentStorageTable(CloudStorageAccount account)
        {
            var client = account
                .CreateCloudTableClient();

            var table = client.GetTableReference(TableName);
            table.DeleteIfExists();
            table.Create();

            return table;
        }

        static CloudTable SetUpAzureStorageTable(CloudStorageAccount account)
        {
            var client = account
                .CreateCloudTableClient();

            var table = client.GetTableReference(TableName);
            table.CreateIfNotExists();

            var query = table.CreateQuery<DynamicTableEntity>();
            var entities = RetrieveAll(table, query);

            var batch = new TableBatchOperation();
            entities.ForEach(batch.Delete);
            table.ExecuteBatch(batch);

            return table;
        }

        static CloudStorageAccount TestStorageAccount()
        {
            var connectionString = Environment.GetEnvironmentVariable(
                "Streamstone-Test-Storage", EnvironmentVariableTarget.User);

            return connectionString != null 
                    ? CloudStorageAccount.Parse(connectionString) 
                    : CloudStorageAccount.DevelopmentStorageAccount;
        }

        public static StreamEntity InsertStreamEntity(this CloudTable table, string partition, int version = 0)
        {
            var entity = new StreamEntity
            {
                PartitionKey = partition,
                RowKey = ApiModel.StreamRowKey,
                Version = version
            };

            table.Execute(TableOperation.Insert(entity));
            return entity;
        }

        public static StreamEntity UpdateStreamEntity(this CloudTable table, string partition, int version = 0)
        {
            var entity = RetrieveStreamEntity(table, partition);
            entity.Version = version;

            table.Execute(TableOperation.Replace(entity));
            return entity;
        }

        public static StreamEntity RetrieveStreamEntity(this CloudTable table, string partition)
        {
            return table.CreateQuery<StreamEntity>()
                        .Where(x =>
                               x.PartitionKey == partition &&
                               x.RowKey == ApiModel.StreamRowKey)
                        .ToList()
                        .SingleOrDefault();
        }

        public static void InsertEventEntities(this CloudTable table, string partition, params string[] ids)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var e = new EventEntity
                {
                    PartitionKey = partition,
                    RowKey = (i+1).FormatEventRowKey(),
                    Id = ids[i],
                };

                table.Execute(TableOperation.Insert(e));
            }
        }

        public static EventEntity[] RetrieveEventEntities(this CloudTable table, string partition)
        {
            return table.CreateQuery<EventEntity>()
                        .Where(x => x.PartitionKey == partition)
                        .Where(RowKeyPrefix.Range<EventEntity>(ApiModel.EventRowKeyPrefix))
                        .ToArray();
        }

        public static void InsertEventIdEntities(this CloudTable table, string partition, params string[] ids)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var e = new EventIdEntity
                {
                    PartitionKey = partition,
                    RowKey = ids[i].FormatEventIdRowKey(),
                };

                table.Execute(TableOperation.Insert(e));
            }
        }

        public static EventIdEntity[] RetrieveEventIdEntities(this CloudTable table, string partition)
        {
            return table.CreateQuery<EventIdEntity>()
                        .Where(x => x.PartitionKey == partition)
                        .Where(RowKeyPrefix.Range<EventIdEntity>(ApiModel.EventIdRowKeyPrefix))
                        .ToArray();
        }

        public static List<DynamicTableEntity> RetrieveAll(this CloudTable table, string partition)
        {
            var query = table.CreateQuery<DynamicTableEntity>()
                             .Where(x => x.PartitionKey == partition);

            return RetrieveAll(table, query);
        }

        static List<DynamicTableEntity> RetrieveAll(CloudTable table, IQueryable<DynamicTableEntity> query)
        {
            var entities = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var page = query.Take(512);

                var segment = table.ExecuteQuerySegmented((TableQuery<DynamicTableEntity>)page, token);
                token = segment.ContinuationToken;
                
                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        public static PartitionContents CaptureContents(this CloudTable table, string partition, Action<PartitionContents> continueWith)
        {
            return new PartitionContents(table, partition, continueWith);
        }

        public class PartitionContents
        {
            readonly CloudTable table;
            readonly string partition;
            readonly List<DynamicTableEntity> captured;

            public PartitionContents(CloudTable table, string partition, Action<PartitionContents> continueWith)
            {
                this.table = table;
                this.partition = partition;

                captured = table.RetrieveAll(partition);
                continueWith(this);
            }

            public void AssertNothingChanged()
            {
                var current = table.RetrieveAll(partition);
                current.ShouldMatch(captured.ToExpectedObject());
            }
        }

        static class RowKeyPrefix
        {
            public static Expression<Func<TEntity, bool>> Range<TEntity>(string prefix) where TEntity : ITableEntity
            {
                var range = new PrefixRange(prefix);

                // ReSharper disable StringCompareToIsCultureSpecific
                return x => x.RowKey.CompareTo(range.Start) >= 0
                            && x.RowKey.CompareTo(range.End) < 0;
            }

            struct PrefixRange
            {
                public readonly string Start;
                public readonly string End;

                public PrefixRange(string prefix)
                {
                    Start = prefix;

                    var length = prefix.Length - 1;
                    var lastChar = prefix[length];
                    var nextLastChar = (char)(lastChar + 1);

                    End = prefix.Substring(0, length) + nextLastChar;
                }
            }
        }
    }
}