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
        const string partition = "test";
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = StorageModel.SetUp();
        }

        [Test]
        public void When_partition_already_contain_stream_header()
        {
            table.InsertStreamEntity(partition);

            Assert.Throws<ConcurrencyConflictException>(
                async ()=> await Stream.ProvisionAsync(table, partition));
        }

        [Test]
        public async void When_partition_is_virgin()
        {
            var stream = await Stream.ProvisionAsync(table, partition);
            var entity = table.RetrieveStreamEntity(partition);
            
            var expectedStream = new Stream
            (
                partition,
                StreamProperties.None,
                entity.ETag,
                start: 0,
                count: 0,
                version: 0
            );

            stream.ShouldEqual(expectedStream.ToExpectedObject());

            var expectedEntity = new
            {
                RowKey = ApiModel.StreamRowKey,
                Start = 0,
                Count = 0,
                Version = 0
            };

            entity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        [Test]
        public async void When_provisioning_along_with_custom_properties()
        {
            var properties = StreamProperties.From(new
            {
                Created = DateTimeOffset.UtcNow,
                Active = true
            });

            var stream = await Stream.ProvisionAsync(table, new Stream(partition, properties));
            var entity = table.RetrieveStreamEntity(partition);

            var expectedStream = new Stream
            (
                partition,
                properties,
                entity.ETag,
                start: 0,
                count: 0,
                version: 0
            );

            stream.ShouldEqual(expectedStream.ToExpectedObject());

            var expectedEntity = new
            {
                RowKey = ApiModel.StreamRowKey,
                Properties = properties,
                Start = 0,
                Count = 0,
                Version = 0
            };

            entity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        [Test]
        public async void When_trying_to_provision_already_stored_stream()
        {
            var stream = await Stream.ProvisionAsync(table, partition);

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ArgumentException>(
                    async () => await Stream.ProvisionAsync(table, stream));

                contents.AssertNothingChanged();
            });
        }
    }
}