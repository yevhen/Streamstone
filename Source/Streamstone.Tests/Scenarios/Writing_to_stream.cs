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
        const string partition = "test"; 
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = StorageModel.SetUp();
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
            var stream = await Stream.ProvisionAsync(table, partition);

            if (streamHeaderChanged)
                table.UpdateStreamEntity(partition, count: 42);

            if (eventEntityExists)
                table.InsertEventEntities(partition, "e123");

            if (idEntityExists)
                table.InsertEventIdEntities(partition, "e123");

            var @event = CreateEvent("e123");

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ConcurrencyConflictException>(
                    async ()=> await Stream.WriteAsync(table, stream, new[] {@event}));
                
                contents.AssertNothingChanged();
            });

            table.UpdateStreamEntity(partition, version: 3);
        }

        [Test]
        public async void When_writing_duplicate_event()
        {
            var stream = await Stream.ProvisionAsync(table, partition);

            table.InsertEventIdEntities(partition, new[] { "e1", "e2" });
            table.CaptureContents(partition, contents =>
            {
                var duplicate = CreateEvent("e2");

                Assert.Throws<DuplicateEventException>(
                    async () => await Stream.WriteAsync(table, stream, new[] {CreateEvent("e3"), duplicate}));

                contents.AssertNothingChanged();  
            });
        }
        
        [Test]
        public async void When_writing_number_of_events_over_max_batch_size_limit()
        {
            var stream = await Stream.ProvisionAsync(table, partition);

            var events = Enumerable
                .Range(1, ApiModel.MaxEntitiesPerBatch + 1)
                .Select(i => CreateEvent("e" + i))
                .ToArray();

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ArgumentOutOfRangeException>(
                    async () => await Stream.WriteAsync(table, stream, events));

                contents.AssertNothingChanged();  
            });
        }

        [Test]
        public async void When_successfully_written_events_to_an_existing_stream()
        {
            var stream = await Stream.ProvisionAsync(table, partition);

            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(table, stream, events);

            AssertModifiedStream(stream, result, start: 1, count: 2, version: 2);
            AssertStreamEntity(start: 1, count: 2, version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertStoredEvent("e1", 1, events[0], storedEvents[0]);
            AssertStoredEvent("e2", 2, events[0], storedEvents[1]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(2));
            
            AssertEventEntity("e1", 1, eventEntities[0]);
            AssertEventEntity("e2", 2, eventEntities[1]);

            var eventIdEntities = table.RetrieveEventIdEntities(partition);
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(table.RetrieveAll(partition).Count, 
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async void When_writing_to_nonexisting_stream()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(table, new Stream(partition), events);

            AssertNewStream(result, start: 1, count: 2, version: 2);
            AssertStreamEntity(start: 1, count: 2, version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertStoredEvent("e1", 1, events[0], storedEvents[0]);
            AssertStoredEvent("e2", 2, events[0], storedEvents[1]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity("e1", 1, eventEntities[0]);
            AssertEventEntity("e2", 2, eventEntities[1]);

            var eventIdEntities = table.RetrieveEventIdEntities(partition);
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(table.RetrieveAll(partition).Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async void When_writing_to_nonexisting_stream_along_with_stream_properties()
        {
            var properties = StreamProperties.From(new Dictionary<string, EntityProperty>
            {
                {"p1", new EntityProperty(42)}, 
                {"p2", new EntityProperty("doh!")}, 
            });

            Event[] events = { CreateEvent("e1"), CreateEvent("e2") };
            var result = await Stream.WriteAsync(table, new Stream(partition, properties), events);

            AssertNewStream(result, start: 1, count: 2, version: 2, properties: properties);
            AssertStreamEntity(start: 1, count: 2, version: 2, properties: properties);
            
            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertStoredEvent("e1", 1, events[0], storedEvents[0]);
            AssertStoredEvent("e2", 2, events[0], storedEvents[1]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity("e1", 1, eventEntities[0]);
            AssertEventEntity("e2", 2, eventEntities[1]);

            var eventIdEntities = table.RetrieveEventIdEntities(partition);
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(table.RetrieveAll(partition).Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        void AssertNewStream(StreamWriteResult actual, int start, int count, int version, StreamProperties properties = null)
        {
            var newStream = actual.Stream;
            var newStreamEntity = table.RetrieveStreamEntity(partition);

            var expectedStream = CreateStream(start, count, version, newStreamEntity.ETag, properties);
            newStream.ShouldEqual(expectedStream.ToExpectedObject());
        }

        void AssertModifiedStream(Stream previous, StreamWriteResult actual, int start, int count, int version)
        {
            var actualStream = actual.Stream;
            var actualStreamEntity = table.RetrieveStreamEntity(partition);

            Assert.That(actualStream.Etag, Is.Not.EqualTo(previous.Etag));
            
            var expectedStream = CreateStream(start, count, version, actualStreamEntity.ETag);
            actualStream.ShouldEqual(expectedStream.ToExpectedObject());
        }

        static Stream CreateStream(int start, int count, int version, string etag, StreamProperties properties = null)
        {
            return new Stream(partition,properties ?? StreamProperties.None, etag, start, count, version);
        }

        void AssertStreamEntity(int start = 0, int count = 0, int version = 0, StreamProperties properties = null)
        {
            var newStreamEntity = table.RetrieveStreamEntity(partition);

            var expectedEntity = new
            {
                RowKey = ApiModel.StreamRowKey,
                Properties = properties ?? StreamProperties.None,
                Start = start,
                Count = count,
                Version = version,
            };

            newStreamEntity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        static void AssertStoredEvent(string id, int version, Event original, StoredEvent actual)
        {
            var expected = new StoredEvent(id, version, original.Properties);
            actual.ShouldMatch(expected.ToExpectedObject());
        }

        static void AssertEventEntity(string id, int version, EventEntity actual)
        {
            var expected = new
            {
                Id = id,
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

        static Event CreateEvent(string id)
        {
            var @event = new TestEventEntity
            {
                Id   = id,
                Type = "StreamChanged",
                Data = "{}"
            };

            return new Event(@event.Id, EventProperties.From(@event));
        }
    }
}