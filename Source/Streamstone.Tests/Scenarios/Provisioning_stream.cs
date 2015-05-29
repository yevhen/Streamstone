using System;
using System.Collections.Generic;
using System.Linq;

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

            Assert.Throws<ConcurrencyConflictException>(
                async ()=> await Stream.ProvisionAsync(partition));
        }

        [Test]
        public async void When_partition_is_virgin()
        {
            var stream = await Stream.ProvisionAsync(partition);
            var entity = partition.RetrieveStreamEntity();
            
            var expectedStream = new Stream(partition, entity.ETag, 0, StreamProperties.None);
            stream.ShouldEqual(expectedStream.ToExpectedObject());

            var expectedEntity = new
            {
                RowKey = Api.StreamRowKey,
                Version = 0
            };

            entity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        [Test]
        public async void When_provisioning_along_with_custom_properties()
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };

            var stream = await Stream.ProvisionAsync(new Stream(partition, properties));
            var entity = partition.RetrieveStreamEntity();

            var expectedStream = new Stream
            (
                partition,
                entity.ETag, 0, 
                StreamProperties.From(properties)
            );

            stream.ShouldEqual(expectedStream.ToExpectedObject());

            var expectedEntity = new
            {
                RowKey = Api.StreamRowKey,
                Properties = StreamProperties.From(properties),
                Version = 0
            };

            entity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        [Test]
        public async void When_trying_to_provision_already_stored_stream()
        {
            var stream = await Stream.ProvisionAsync(partition);

            partition.CaptureContents(contents =>
            {
                Assert.Throws<ArgumentException>(
                    async () => await Stream.ProvisionAsync(stream));

                contents.AssertNothingChanged();
            });
        }
    }
}