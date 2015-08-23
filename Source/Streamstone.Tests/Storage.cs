using System;
using System.Collections.Generic;
using System.Linq;

using ExpectedObjects;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone
{
    using Utility;

    static class Storage
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

        public static StreamEntity InsertStreamEntity(this Partition partition, int version = 0)
        {
            var entity = new StreamEntity
            {
                PartitionKey = partition.PartitionKey,
                RowKey = Api.StreamRowKey,
                Version = version
            };

            partition.Table.Execute(TableOperation.Insert(entity));
            return entity;
        }

        public static StreamEntity UpdateStreamEntity(this Partition partition, int version = 0)
        {
            var entity = RetrieveStreamEntity(partition);
            entity.Version = version;

            partition.Table.Execute(TableOperation.Replace(entity));
            return entity;
        }

        public static StreamEntity RetrieveStreamEntity(this Partition partition)
        {
            return partition.Table.CreateQuery<StreamEntity>()
                        .Where(x =>
                               x.PartitionKey == partition.PartitionKey &&
                               x.RowKey == Api.StreamRowKey)
                        .ToList()
                        .SingleOrDefault();
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

                partition.Table.Execute(TableOperation.Insert(e));
            }
        }

        public static EventEntity[] RetrieveEventEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<EventEntity>(prefix: Api.EventRowKeyPrefix).ToArray();
        }

        public static void InsertEventIdEntities(this Partition partition, params string[] ids)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var e = new EventIdEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = ids[i].FormatEventIdRowKey(),
                };

                partition.Table.Execute(TableOperation.Insert(e));
            }
        }

        public static EventIdEntity[] RetrieveEventIdEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<EventIdEntity>(prefix: Api.EventIdRowKeyPrefix).ToArray();
        }

        public static List<DynamicTableEntity> RetrieveAll(this Partition partition)
        {
            var query = partition.Table.CreateQuery<DynamicTableEntity>()
                                 .Where(x => x.PartitionKey == partition.PartitionKey);

            return RetrieveAll(partition.Table, query);
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

        public static PartitionContents CaptureContents(this Partition partition, Action<PartitionContents> continueWith)
        {
            return new PartitionContents(partition, continueWith);
        }

        public class PartitionContents
        {
            readonly CloudTable table;
            readonly Partition partition;
            readonly List<DynamicTableEntity> captured;

            public PartitionContents(Partition partition, Action<PartitionContents> continueWith)
            {
                this.table = partition.Table;
                this.partition = partition;

                captured = partition.RetrieveAll();
                continueWith(this);
            }

            public void AssertNothingChanged()
            {
                var current = partition.RetrieveAll();
                current.ShouldMatch(captured.ToExpectedObject());
            }
        }
    }
}