using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Including_additional_entities
    {
        const string partition = "test";
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
        }

        [Test]
        public void When_writing_number_of_events_plus_includes_is_over_max_batch_size_limit()
        {
            var events = Enumerable
                .Range(1, Api.MaxEventsPerBatch - 1)
                .Select(i => CreateEvent("e" + i))
                .ToArray();

            var includes = Enumerable
                .Range(1, Api.MaxEntitiesTotalPerBatch - events.Length * 2  + 1)
                .Select(i => Include.Insert(new TestEntity()))
                .ToArray();

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ArgumentOutOfRangeException>(
                    async () => await Stream.WriteAsync(table, new Stream(partition), events, includes));

                contents.AssertNothingChanged();
            });
        }

        [Test]
        public async void When_include_has_no_conflicts()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            await Stream.WriteAsync(table, new Stream(partition), events, new[]{include});
            
            var actual = RetrieveTestEntity(entity.RowKey);
            Assert.That(actual, Is.Not.Null);
        }
        
        [Test]
        public async void When_include_has_conflict()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            var result = await Stream.WriteAsync(table, new Stream(partition), events, new[]{include});

            events = new[] {CreateEvent("e3")};
            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            Assert.Throws<IncludedOperationConflictException>(
                async ()=> await Stream.WriteAsync(table, result.Stream, events, new[] {include}));
        }

        [Test]
        public async void When_include_has_conflict_and_also_duplicate_event_conflict()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};

            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            var result = await Stream.WriteAsync(table, new Stream(partition), events, new[]{include});

            events = new[] {CreateEvent("e1")};
            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            Assert.Throws<DuplicateEventException>(
                async () => await Stream.WriteAsync(table, result.Stream, events, new[]{include}));
        }

        [Test]
        public async void When_include_has_conflict_and_also_stream_header_has_changed_since_last_read()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};

            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            var result = await Stream.WriteAsync(table, new Stream(partition), events, new[]{include});

            events = new[] {CreateEvent("e3")};
            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            table.UpdateStreamEntity(partition);
            
            Assert.Throws<ConcurrencyConflictException>(
                async () => await Stream.WriteAsync(table, result.Stream, events, new[]{include}));
        }        
        
        [Test]
        public async void When_included_entity_implements_versioned_entity()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};

            var entity  = new TestVersionedEntity("INV-0001");
            var include = Include.Insert(entity);

            var result = await Stream.WriteAsync(table, new Stream(partition), events, new[]{include});
            Assert.That(entity.Version, Is.EqualTo(result.Stream.Version));
        }

        TestEntity RetrieveTestEntity(string rowKey)
        {
            return table.CreateQuery<TestEntity>()
                        .Where(x =>
                               x.PartitionKey == partition
                               && x.RowKey == rowKey)
                        .ToList()
                        .SingleOrDefault();
        }

        static Event CreateEvent(string id)
        {
            return new Event(id, new Dictionary<string, EntityProperty>
            {
                {"Id",   new EntityProperty(id)},
                {"Type", new EntityProperty("StreamChanged")},
                {"Data", new EntityProperty("{}")}
            });
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

        class TestVersionedEntity : TestEntity, IVersionedEntity
        {
            public TestVersionedEntity(string rowKey)
                : base(rowKey)
            {}

            public int Version { get; set; }
        }
    }
}