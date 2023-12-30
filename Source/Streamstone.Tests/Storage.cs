using System;
using System.Collections.Generic;
using System.Linq;

using Azure.Data.Tables;

using ExpectedObjects;

namespace Streamstone
{
    using Utility;

    static class Storage
    {
        const string TableName = "Streams";
        const string DevelopmentConnectionString = "UseDevelopmentStorage=true";

        public static bool IsAzurite() => TestStorageAccount() == DevelopmentConnectionString;

        public static TableClient SetUp()
        {
            var connectionString = TestStorageAccount();

            return connectionString == DevelopmentConnectionString
                    ? SetUpDevelopmentStorageTable(connectionString)
                    : SetUpAzureStorageTable(connectionString);
        }

        static TableClient SetUpDevelopmentStorageTable(string connectionString)
        {
            var client = new TableServiceClient(connectionString);

            var table = client.GetTableClient(TableName);
            table.Delete();
            table.Create();

            return table;
        }

        static TableClient SetUpAzureStorageTable(string connectionString)
        {
            var client = new TableServiceClient(connectionString);

            var table = client.GetTableClient(TableName);
            table.CreateIfNotExists();

            var entities = RetrieveAll(table);
            if (entities.Count == 0)
                return table;

            var partitions = entities.GroupBy(x => x.PartitionKey).ToList();

            foreach (var partition in partitions.Select(x => x.ToList()))
            {
                const int maxBatchSize = 100;
                var batches = (int)Math.Ceiling((double)partition.Count / maxBatchSize);
                foreach (var batch in Enumerable.Range(0, batches))
                {
                    var operations = new List<TableTransactionAction>();
                    var slice = partition.Skip(batch * maxBatchSize).Take(maxBatchSize).ToList();
                    slice.ForEach(entity => operations.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity)));
                    table.SubmitTransaction(operations);
                }
            }

            return table;
        }

        static string TestStorageAccount()
        {
            var connectionString = Environment.GetEnvironmentVariable(
                "Streamstone-Test-Storage", EnvironmentVariableTarget.User);

            return connectionString ?? DevelopmentConnectionString;
        }

        public static StreamEntity InsertStreamEntity(this Partition partition, int version = 0)
        {
            var entity = new StreamEntity
            {
                PartitionKey = partition.PartitionKey,
                RowKey = Api.StreamRowKey,
                Version = version
            };

            partition.Table.AddEntity(entity.ToTableEntity());
            return entity;
        }

        public static StreamEntity UpdateStreamEntity(this Partition partition, int version = 0)
        {
            var entity = RetrieveStreamEntity(partition);
            entity.Version = version;

            var response = partition.Table.UpdateEntity(entity.ToTableEntity(), entity.ETag, TableUpdateMode.Replace);
            entity.ETag = response.Headers.ETag.Value;
            return entity;
        }

        public static StreamEntity RetrieveStreamEntity(this Partition partition)
        {
            var entity = partition.Table.GetEntity<TableEntity>(partition.PartitionKey, Api.StreamRowKey);
            return StreamEntity.From(entity);
        }

        public static void InsertEventEntities(this Partition partition, params string[] ids)
        {
            for (var i = 0; i < ids.Length; i++)
            {
                var e = new EventEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = (i+1).FormatEventRowKey()
                };

                partition.Table.AddEntity(e.ToTableEntity());
            }
        }

        public static EventEntity[] RetrieveEventEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<TableEntity>(prefix: Api.EventRowKeyPrefix)
                .Select(EventEntity.From)
                .ToArray();
        }

        public static void InsertEventIdEntities(this Partition partition, params string[] ids)
        {
            foreach (var id in ids)
            {
                var entity = new EventIdEntity
                {
                    PartitionKey = partition.PartitionKey,
                    RowKey = id.FormatEventIdRowKey(),
                };

                partition.Table.AddEntity(entity.ToTableEntity());
            }
        }

        public static EventIdEntity[] RetrieveEventIdEntities(this Partition partition)
        {
            return partition.RowKeyPrefixQuery<TableEntity>(prefix: Api.EventIdRowKeyPrefix)
                .Select(EventIdEntity.From)
                .ToArray();
        }

        public static List<TableEntity> RetrieveAll(this Partition partition)
        {
            var filter = TableClient.CreateQueryFilter($"PartitionKey eq {partition.PartitionKey}");

            return partition.Table.Query<TableEntity>(filter).ToList();
        }

        static List<TableEntity> RetrieveAll(TableClient table)
        {
            return table.Query<TableEntity>().ToList();
        }

        public static PartitionContents CaptureContents(this Partition partition, Action<PartitionContents> continueWith)
        {
            return new PartitionContents(partition, continueWith);
        }

        public class PartitionContents
        {
            readonly Partition partition;
            readonly List<TableEntity> captured;

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