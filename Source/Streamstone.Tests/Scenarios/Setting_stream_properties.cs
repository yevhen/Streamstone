using System;
using System.Linq;

using ExpectedObjects;
using NUnit.Framework;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Setting_stream_properties
    {
        const string partition = "test";
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = StorageModel.SetUp();
        }

        [Test]
        public async void When_property_map_is_empty()
        {
            var properties = new{};

            var previous = await Stream.ProvisionAsync(table, partition);
            var current  = await Stream.SetPropertiesAsync(table, previous, properties);
            
            Assert.That(current.ETag, Is.Not.EqualTo(previous.ETag));
            StreamProperties.From(properties).ToExpectedObject().ShouldEqual(current.Properties);
        } 

        [Test]
        public async void When_concurrency_conflict()
        {
            var stream = await Stream.ProvisionAsync(table, partition);
            table.UpdateStreamEntity(partition, start: 1);

            Assert.Throws<ConcurrencyConflictException>(
                async ()=> await Stream.SetPropertiesAsync(table, stream, new{}));
        }

        [Test]
        public async void When_set_successfully()
        {
            var properties = new {p1 = 42, p2 = "42"};
            var stream = await Stream.ProvisionAsync(table, new Stream(partition, properties));

            var newProperties = new {p1 = 56, p3 = "56"};
            var newStream = await Stream.SetPropertiesAsync(table, stream, newProperties);

            StreamProperties.From(properties).ToExpectedObject().ShouldEqual(newStream.Properties);

            var storedEntity = table.RetrieveStreamEntity(partition);
            var storedProperties = storedEntity.Properties;

            StreamProperties.From(properties).ToExpectedObject().ShouldEqual(storedProperties);
        }

        [Test]
        public void When_trying_to_set_properties_on_transient_stream()
        {
            var stream = new Stream(partition);           

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ArgumentException>(
                    async () => await Stream.SetPropertiesAsync(table, stream, new{}));

                contents.AssertNothingChanged();
            });
        }
    }
}