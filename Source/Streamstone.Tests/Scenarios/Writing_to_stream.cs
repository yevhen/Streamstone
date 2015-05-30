using System;
using System.Linq;
using System.Collections.Generic;

using NUnit.Framework;
using ExpectedObjects;

using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Writing_to_stream
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
        [TestCase(true,  false, false, Description = "Only Header changed. Concurrent update of stream properties")]
        [TestCase(true,  true,  false, Description = "Header + Event with the same version exists. Concurrent append")]
        [TestCase(true,  false, true,  Description = "Header + ID exists. Concurrent append")]
        [TestCase(true,  true,  true,  Description = "Header + Event + ID. Concurrent append")]
        [TestCase(false, true,  false, Description = "Only Event with the same version exists. Degenerate case (corruption or manual edit)")]
        [TestCase(false, true,  true,  Description = "Event + ID. Degenerate case (corruption or manual edit)")]
        public async void When_write_conflict(bool streamHeaderChanged, bool eventEntityExists, bool idEntityExists)
        {
            var stream = await Stream.ProvisionAsync(partition);

            if (streamHeaderChanged)
                partition.UpdateStreamEntity();

            if (eventEntityExists)
                partition.InsertEventEntities("e123");

            if (idEntityExists)
                partition.InsertEventIdEntities("e123");

            var @event = CreateEvent("e123");

            partition.CaptureContents(contents =>
            {
                Assert.Throws<ConcurrencyConflictException>(
                    async ()=> await Stream.WriteAsync(stream, new[] {@event}));
                
                contents.AssertNothingChanged();
            });

            partition.UpdateStreamEntity(version: 3);
        }

        [Test]
        public async void When_writing_duplicate_event()
        {
            var stream = await Stream.ProvisionAsync(partition);

            partition.InsertEventIdEntities(new[] { "e1", "e2" });
            partition.CaptureContents(contents =>
            {
                var duplicate = CreateEvent("e2");

                Assert.Throws<DuplicateEventException>(
                    async () => await Stream.WriteAsync(stream, new[] {CreateEvent("e3"), duplicate}));

                contents.AssertNothingChanged();  
            });
        }
        
        [Test]
        public async void When_writing_number_of_events_over_max_batch_size_limit()
        {
            var stream = await Stream.ProvisionAsync(partition);

            var events = Enumerable
                .Range(1, Api.MaxEventsPerBatch + 1)
                .Select(i => CreateEvent("e" + i))
                .ToArray();

            partition.CaptureContents(contents =>
            {
                Assert.Throws<ArgumentOutOfRangeException>(
                    async () => await Stream.WriteAsync(stream, events));

                contents.AssertNothingChanged();  
            });
        }

        [Test]
        public async void When_successfully_written_events_to_an_existing_stream()
        {
            var stream = await Stream.ProvisionAsync(partition);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(stream, events);

            AssertModifiedStream(stream, result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));
            
            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(partition.RetrieveAll().Count, 
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async void When_writing_to_nonexisting_stream()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            AssertNewStream(result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async void When_writing_to_nonexisting_stream_along_with_stream_properties()
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };
            
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(new Stream(partition, properties), events);

            AssertNewStream(result, 2, properties);
            AssertStreamEntity(2, properties);
            
            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        void AssertNewStream(StreamWriteResult actual, int version, Dictionary<string, EntityProperty> properties = null)
        {
            var newStream = actual.Stream;
            var newStreamEntity = partition.RetrieveStreamEntity();

            var expectedStream = CreateStream(version, newStreamEntity.ETag, properties);
            newStream.ShouldEqual(expectedStream.ToExpectedObject());
        }

        void AssertModifiedStream(Stream previous, StreamWriteResult actual, int version)
        {
            var actualStream = actual.Stream;
            var actualStreamEntity = partition.RetrieveStreamEntity();

            Assert.That(actualStream.ETag, Is.Not.EqualTo(previous.ETag));
            
            var expectedStream = CreateStream(version, actualStreamEntity.ETag);
            actualStream.ShouldEqual(expectedStream.ToExpectedObject());
        }

        Stream CreateStream(int version, string etag, IDictionary<string, EntityProperty> properties = null)
        {
            var props = properties != null  
                ? StreamProperties.From(properties) 
                : StreamProperties.None;

            return new Stream(partition, etag, version, props);
        }

        void AssertStreamEntity(int version = 0, Dictionary<string, EntityProperty> properties = null)
        {
            var newStreamEntity = partition.RetrieveStreamEntity();

            var expectedEntity = new
            {
                RowKey = Api.StreamRowKey,
                Properties = properties != null
                    ? StreamProperties.From(properties)
                    : StreamProperties.None,
                Version = version,
            };

            newStreamEntity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        static void AssertRecordedEvent(int version, EventData source, RecordedEvent actual)
        {
            var expected = source.Record(version);
            actual.ShouldMatch(expected.ToExpectedObject());
        }

        static void AssertEventEntity(int version, EventEntity actual)
        {
            var expected = new
            {
                RowKey = version.FormatEventRowKey(),
                Properties = EventProperties.From(new Dictionary<string, EntityProperty>
                {
                    {"Type", new EntityProperty("StreamChanged")},
                    {"Data", new EntityProperty("{}")},
                }),
                Version = version,
            };

            actual.ShouldMatch(expected.ToExpectedObject());
        }

        static void AssertEventIdEntity(string id, int version, EventIdEntity actual)
        {
            var expected = new
            {
                RowKey = id.FormatEventIdRowKey(),
                Version = version,
            };

            actual.ShouldMatch(expected.ToExpectedObject());
        }

        static EventData CreateEvent(string id)
        {
            return new EventData(id, new Dictionary<string, EntityProperty>
            {
                {"Type", new EntityProperty("StreamChanged")},
                {"Data", new EntityProperty("{}")}
            });
        }
    }
}