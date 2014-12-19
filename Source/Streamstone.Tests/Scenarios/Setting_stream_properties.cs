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
            var properties = StreamProperties.From(new{});

            var previous = await Stream.ProvisionAsync(table, partition);
            var current  = await Stream.SetPropertiesAsync(table, previous, properties);
            
            Assert.That(current.Etag, Is.Not.EqualTo(previous.Etag));
            properties.ToExpectedObject().ShouldEqual(current.Properties);
        } 

        [Test]
        public async void When_concurrency_conflict()
        {
            var properties = StreamProperties.From(new {});

            var stream = await Stream.ProvisionAsync(table, partition);
            table.UpdateStreamEntity(partition, start: 1);

            Assert.Throws<ConcurrencyConflictException>(
                async ()=> await Stream.SetPropertiesAsync(table, stream, properties));
        }

        [Test]
        public async void When_set_successfully()
        {
            var properties = StreamProperties.From(new 
            {
                p1 = 42,
                p2 = "42"
            });

            var stream = await Stream.ProvisionAsync(table, new Stream(partition, properties));

            var newProperties = StreamProperties.From(new
            {
                p1 = 56,
                p3 = "56"
            });

            var newStream = await Stream.SetPropertiesAsync(table, stream, newProperties);

            newProperties.ToExpectedObject().ShouldEqual(newStream.Properties);

            var storedEntity = table.RetrieveStreamEntity(partition);
            var storedProperties = storedEntity.Properties;

            newProperties.ToExpectedObject().ShouldEqual(storedProperties);
        }

        [Test]
        public void When_trying_to_set_properties_on_transient_stream()
        {
            var stream = new Stream(partition);           
            var properties = StreamProperties.From(new {p1 = 42, p2 = "42"});

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ArgumentException>(
                    async () => await Stream.SetPropertiesAsync(table, stream, properties));

                contents.AssertNothingChanged();
            });
        }
    }
}