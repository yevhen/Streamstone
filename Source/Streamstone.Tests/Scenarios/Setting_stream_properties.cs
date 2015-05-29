using System;
using System.Collections.Generic;
using System.Linq;

using ExpectedObjects;
using NUnit.Framework;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Setting_stream_properties
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
        public async void When_property_map_is_empty()
        {
            var properties = new Dictionary<string, EntityProperty>();

            var previous = await Stream.ProvisionAsync(partition);
            var current  = await Stream.SetPropertiesAsync(previous, properties);
            
            Assert.That(current.ETag, Is.Not.EqualTo(previous.ETag));
            StreamProperties.From(properties).ToExpectedObject().ShouldEqual(current.Properties);
        } 

        [Test]
        public async void When_concurrency_conflict()
        {
            var stream = await Stream.ProvisionAsync(partition);
            partition.UpdateStreamEntity();

            Assert.Throws<ConcurrencyConflictException>(
                async ()=> await Stream.SetPropertiesAsync(stream, new Dictionary<string, EntityProperty>()));
        }

        [Test]
        public async void When_set_successfully()
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"P1", new EntityProperty(42)},
                {"P2", new EntityProperty("42")}
            };

            var stream = await Stream.ProvisionAsync(new Stream(partition, properties));

            var newProperties = new Dictionary<string, EntityProperty>
            {
                {"P1", new EntityProperty(56)},
                {"P2", new EntityProperty("56")}
            };

            var newStream = await Stream.SetPropertiesAsync(stream, newProperties);
            StreamProperties.From(newProperties).ToExpectedObject().ShouldEqual(newStream.Properties);

            var storedEntity = partition.RetrieveStreamEntity();
            var storedProperties = storedEntity.Properties;

            StreamProperties.From(newProperties).ToExpectedObject().ShouldEqual(storedProperties);
        }

        [Test]
        public void When_trying_to_set_properties_on_transient_stream()
        {
            var stream = new Stream(partition);           

            partition.CaptureContents(contents =>
            {
                Assert.Throws<ArgumentException>(
                    async ()=> await Stream.SetPropertiesAsync(stream, new Dictionary<string, EntityProperty>()));

                contents.AssertNothingChanged();
            });
        }
    }
}