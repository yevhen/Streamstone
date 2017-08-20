using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;
using ExpectedObjects;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Provisioning_stream
    {
        Partition partition;
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
            partition = new Partition(table, "test");
        }

        [Test]
        public void When_partition_already_contain_stream_header()
        {
            partition.InsertStreamEntity();

            Assert.ThrowsAsync<ConcurrencyConflictException>(
                async ()=> await Stream.ProvisionAsync(partition));
        }

        [Test]
        public async Task When_partition_is_virgin()
        {
            var stream = await Stream.ProvisionAsync(partition);
            var entity = partition.RetrieveStreamEntity();
            
            var expectedStream = new Stream(partition, entity.ETag, 0, StreamProperties.None);
            expectedStream.ToExpectedObject().ShouldEqual(stream);

            var expectedEntity = new
            {
                RowKey = Api.StreamRowKey,
                Version = 0
            };

            expectedEntity.ToExpectedObject().ShouldMatch(entity);
        }

        [Test]
        public async Task When_provisioning_along_with_custom_properties()
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };

            var stream = await Stream.ProvisionAsync(partition, StreamProperties.From(properties));
            var entity = partition.RetrieveStreamEntity();

            var expectedStream = new Stream
            (
                partition,
                entity.ETag, 0, 
                StreamProperties.From(properties)
            );

            expectedStream.ToExpectedObject().ShouldEqual(stream);

            var expectedEntity = new
            {
                RowKey = Api.StreamRowKey,
                Properties = StreamProperties.From(properties),
                Version = 0
            };

            expectedEntity.ToExpectedObject().ShouldMatch(entity);
        }
    }
}