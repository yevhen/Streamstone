using System;
using System.Collections.Generic;
using System.Linq;

using ExpectedObjects;

using Microsoft.Azure.Cosmos.Table;

namespace Streamstone
{
    using Utility;
    static class Storage
    {
        const string TableName = "Streams";

        public static bool IsAzurite() => Environment.GetEnvironmentVariable("Streamstone-Azurite", EnvironmentVariableTarget.User) == "true";

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
            table.DeleteIfExistsAsync().Wait();
            table.CreateAsync().Wait();

            return table;
        }

        static CloudTable SetUpAzureStorageTable(CloudStorageAccount account)
        {
            var client = account
                .CreateCloudTableClient();

            var table = client.GetTableReference(TableName);
            table.CreateIfNotExistsAsync().Wait();

            var entities = RetrieveAll(table);
            if (entities.Count == 0)
                return table;

            const int maxBatchSize = 100;
            var batches = (int)Math.Ceiling((double)entities.Count / maxBatchSize);
            foreach (var batch in Enumerable.Range(0, batches))
            {
                var operation = new TableBatchOperation();
                var slice = entities.Skip(batch * maxBatchSize).Take(maxBatchSize).ToList();
                slice.ForEach(operation.Delete);
                table.ExecuteBatchAsync(operation).Wait();
            }

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

        public static StreamEntity InsertStreamEntity(this Partition partition, int version = 0)
        {
            var entity = new StreamEntity
            {
                PartitionKey = partition.PartitionKey,
                RowKey = Api.StreamRowKey,
                Version = version
            };

            partition.Table.ExecuteAsync(TableOperation.Insert(entity)).Wait();
            return entity;
        }

        public static StreamEntity UpdateStreamEntity(this Partition partition, int version = 0)
        {
            var entity = RetrieveStreamEntity(partition);
            entity.Version = version;

            partition.Table.ExecuteAsync(TableOperation.Replace(entity)).Wait();
            return entity;
        }

        public static StreamEntity RetrieveStreamEntity(this Partition partition)
        {
            var filter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(nameof(StreamEntity.PartitionKey), QueryComparisons.Equal, partition.PartitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(nameof(StreamEntity.RowKey), QueryComparisons.Equal, Api.StreamRowKey));

            var query = new TableQuery<StreamEntity>().Where(filter);

            var segment = partition.Table.ExecuteQuerySegmentedAsync(query, null).Result;
            return segment.SingleOrDefault();
        }

        public static void InsertEventEntities(this Partition partition, params string[] ids)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var e = new EventEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = (i+1).FormatEventRowKey()
                };

                partition.Table.ExecuteAsync(TableOperation.Insert(e)).Wait();
            }
        }

        public static EventEntity[] RetrieveEventEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<EventEntity>(prefix: Api.EventRowKeyPrefix).ToArray();
        }

        public static void InsertEventIdEntities(this Partition partition, params string[] ids)
        {
            foreach (var id in ids)
            {
                var e = new EventIdEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = id.FormatEventIdRowKey(),
                };

                partition.Table.ExecuteAsync(TableOperation.Insert(e)).Wait();
            }
        }

        public static EventIdEntity[] RetrieveEventIdEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<EventIdEntity>(prefix: Api.EventIdRowKeyPrefix).ToArray();
        }

        public static List<DynamicTableEntity> RetrieveAll(this Partition partition)
        {
            var filter = TableQuery.GenerateFilterCondition(nameof(StreamEntity.PartitionKey), QueryComparisons.Equal, partition.PartitionKey);
            var query = new TableQuery<DynamicTableEntity>().Where(filter);

            var entities = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var segment = partition.Table.ExecuteQuerySegmentedAsync(query, token).Result;
                token = segment.ContinuationToken;

                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        static List<DynamicTableEntity> RetrieveAll(CloudTable table)
        {
            var entities = new List<DynamicTableEntity>();
            TableContinuationToken token = null;

            do
            {
                var page = new TableQuery<DynamicTableEntity>().Take(512); 

                var segment = table.ExecuteQuerySegmentedAsync(page, token).Result;
                token = segment.ContinuationToken;
                
                entities.AddRange(segment.Results);
            }
            while (token != null);

            return entities;
        }

        public static PartitionContents CaptureContents(this Partition partition, Action<PartitionContents> continueWith)
        {
            return new PartitionContents(partition, continueWith);
        }

        public class PartitionContents
        {
            readonly Partition partition;
            readonly List<DynamicTableEntity> captured;

            public PartitionContents(Partition partition, Action<PartitionContents> continueWith)
            {
                this.partition = partition;

                captured = partition.RetrieveAll();
                continueWith(this);
            }

            public void AssertNothingChanged()
            {
                var current = partition.RetrieveAll();
                captured.ToExpectedObject().ShouldMatch(current);
            }
        }
    }
}