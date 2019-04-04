using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos.Table;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    using Utility;

    [TestFixture]
    public class Including_additional_entities
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
        public async Task When_include_has_no_conflicts_happy_path()
        {
            var entity1 = new TestEntity("INV-0001");
            var entity2 = new TestEntity("INV-0002");

            EventData[] events =
            {
                CreateEvent("e1", Include.Insert(entity1)), 
                CreateEvent("e2", Include.Insert(entity2))
            };

            var result = await Stream.WriteAsync(new Stream(partition), events);

            var stored = RetrieveTestEntity(entity1.RowKey);
            Assert.That(stored, Is.Not.Null);

            stored = RetrieveTestEntity(entity2.RowKey);
            Assert.That(stored, Is.Not.Null);

            Assert.That(result.Includes.Length, Is.EqualTo(2));
            Assert.That(result.Includes[0], Is.SameAs(entity1));
            Assert.That(result.Includes[0].ETag, Is.Not.Null.Or.Empty);
            Assert.That(result.Includes[1], Is.SameAs(entity2));
            Assert.That(result.Includes[1].ETag, Is.Not.Null.Or.Empty);            
        }
        
        [Test]
        public async Task When_include_has_conflict()
        {
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            events = new[] {CreateEvent("e3", include)};
            Assert.ThrowsAsync<IncludedOperationConflictException>(
                async ()=> await Stream.WriteAsync(result.Stream, events));
        }

        [Test]
        public async Task When_include_has_conflict_and_also_duplicate_event_conflict()
        {
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            events = new[] {CreateEvent("e1", include)};
            Assert.ThrowsAsync<DuplicateEventException>(
                async () => await Stream.WriteAsync(result.Stream, events));
        }

        [Test]
        public async Task When_include_has_conflict_and_also_stream_header_has_changed_since_last_read()
        {
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            partition.UpdateStreamEntity();

            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            events = new[] {CreateEvent("e3", include)};
            Assert.ThrowsAsync<ConcurrencyConflictException>(
                async () => await Stream.WriteAsync(result.Stream, events));
        }

        [Test]
        public async Task When_in_total_with_includes_is_over_WATS_max_batch_size_limit()
        {
            var stream = new Stream(partition);

            var events = Enumerable
                .Range(1, Api.AzureMaxBatchSize + 1)
                .Select(i => CreateEvent("e" + i, Include.Insert(new TestEntity(i.ToString()))))
                .ToArray();

            var result = await Stream.WriteAsync(stream, events);

            var storedEvents = result.Events;
            Assert.That(storedEvents.Length, Is.EqualTo(events.Length));

            var eventEntities = partition.RetrieveEventEntities();
            Assert.That(eventEntities.Length, Is.EqualTo(events.Length));

            var eventIdEntities = partition.RetrieveEventIdEntities();
            Assert.That(eventIdEntities.Length, Is.EqualTo(events.Length));

            Assert.That(partition.RetrieveAll().Count,
                Is.EqualTo((eventEntities.Length * 2) + eventIdEntities.Length + 1));
        }

        [Test]
        public void When_single_event_along_with_includes_is_over_WATS_max_batch_size_limit()
        {
            var stream = new Stream(partition);

            var includes = Enumerable
                .Range(1, Api.AzureMaxBatchSize)
                .Select(i => Include.Insert(new TestEntity(i.ToString())))
                .ToArray();

            var @event = new EventData(
                EventId.From("offsize"), 
                EventIncludes.From(includes)
            );

            Assert.ThrowsAsync<InvalidOperationException>(
                async ()=> await Stream.WriteAsync(stream, @event));
        }

        TestEntity RetrieveTestEntity(string rowKey)
        {
            var filter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition.PartitionKey),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            return table.ExecuteQuery<TestEntity>(filter)
                        .SingleOrDefault();
        }

        static EventData CreateEvent(string id, params Include[] includes)
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Id",   new EntityProperty(id)},
                {"Type", new EntityProperty("StreamChanged")},
                {"Data", new EntityProperty("{}")}
            };

            return new EventData(
                            EventId.From(id), 
                            EventProperties.From(properties), 
                            EventIncludes.From(includes));
        }

        class TestEntity : TableEntity
        {
            public TestEntity()
            {}

            public TestEntity(string rowKey)
            {
                RowKey = rowKey;
                Data = DateTime.UtcNow.ToString();
            }

            public string Data { get; set; }            
        }
    }
}