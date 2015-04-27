﻿using System;
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
                table.UpdateStreamEntity(partition);

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
            var eventsCount = ApiModel.MaxEntitiesPerBatch + 1;

            var events = Enumerable
                .Range(1, eventsCount)
                .Select(i => CreateEvent("e" + i))
                .ToArray();

            var result = await Stream.WriteAsync(table, stream, events);

            AssertNewStream(result, version: eventsCount);
            AssertStreamEntity(version: eventsCount);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(eventsCount));

            for (var i = 0; i < eventsCount; ++i)
                AssertRecordedEvent(i+1, events[i], storedEvents[i]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(eventsCount));

            for (var i = 0; i < eventsCount; ++i)
                AssertEventEntity(i+1, eventEntities[i]);

            var eventIdEntities = table.RetrieveEventIdEntities(partition);
            Assert.That(eventIdEntities.Length, Is.EqualTo(eventsCount));

            for (var i = 0; i < eventsCount; ++i)
                AssertEventIdEntity("e" + (i+1), i+1, eventIdEntities[i]);

            Assert.That(table.RetrieveAll(partition).Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        [Test]
        public async void When_successfully_written_events_to_an_existing_stream()
        {
            var stream = await Stream.ProvisionAsync(table, partition);

            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(table, stream, events);

            AssertModifiedStream(stream, result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(2));
            
            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

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

            AssertNewStream(result, version: 2);
            AssertStreamEntity(version: 2);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

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
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Created", new EntityProperty(DateTimeOffset.Now)},
                {"Active",  new EntityProperty(true)}
            };
            
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            var result = await Stream.WriteAsync(table, new Stream(partition, properties), events);

            AssertNewStream(result, 2, properties);
            AssertStreamEntity(2, properties);
            
            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(2));

            AssertRecordedEvent(1, events[0], storedEvents[0]);
            AssertRecordedEvent(2, events[1], storedEvents[1]);

            var eventEntities = table.RetrieveEventEntities(partition);
            Assert.That(eventEntities.Length, Is.EqualTo(2));

            AssertEventEntity(1, eventEntities[0]);
            AssertEventEntity(2, eventEntities[1]);

            var eventIdEntities = table.RetrieveEventIdEntities(partition);
            Assert.That(eventIdEntities.Length, Is.EqualTo(2));

            AssertEventIdEntity("e1", 1, eventIdEntities[0]);
            AssertEventIdEntity("e2", 2, eventIdEntities[1]);

            Assert.That(table.RetrieveAll(partition).Count,
                Is.EqualTo(eventEntities.Length + eventIdEntities.Length + 1));
        }

        void AssertNewStream(StreamWriteResult actual, int version, Dictionary<string, EntityProperty> properties = null)
        {
            var newStream = actual.Stream;
            var newStreamEntity = table.RetrieveStreamEntity(partition);

            var expectedStream = CreateStream(version, newStreamEntity.ETag, properties);
            newStream.ShouldEqual(expectedStream.ToExpectedObject());
        }

        void AssertModifiedStream(Stream previous, StreamWriteResult actual, int version)
        {
            var actualStream = actual.Stream;
            var actualStreamEntity = table.RetrieveStreamEntity(partition);

            Assert.That(actualStream.ETag, Is.Not.EqualTo(previous.ETag));
            
            var expectedStream = CreateStream(version, actualStreamEntity.ETag);
            actualStream.ShouldEqual(expectedStream.ToExpectedObject());
        }

        static Stream CreateStream(int version, string etag, IDictionary<string, EntityProperty> properties = null)
        {
            return new Stream(partition, etag, version, properties != null 
                                                            ? StreamProperties.From(properties) 
                                                            : StreamProperties.None);
        }

        void AssertStreamEntity(int version = 0, Dictionary<string, EntityProperty> properties = null)
        {
            var newStreamEntity = table.RetrieveStreamEntity(partition);

            var expectedEntity = new
            {
                RowKey = ApiModel.StreamRowKey,
                Properties = properties != null
                    ? StreamProperties.From(properties)
                    : StreamProperties.None,
                Version = version,
            };

            newStreamEntity.ShouldMatch(expectedEntity.ToExpectedObject());
        }

        static void AssertRecordedEvent(int version, Event source, RecordedEvent actual)
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

        static Event CreateEvent(string id)
        {
            return new Event(id, new Dictionary<string, EntityProperty>
            {
                {"Type", new EntityProperty("StreamChanged")},
                {"Data", new EntityProperty("{}")}
            });
        }
    }
}