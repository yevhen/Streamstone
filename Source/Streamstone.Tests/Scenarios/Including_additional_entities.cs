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
        Partition partition;
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
            partition = new Partition(table, "test");
        }

        [Test]
        public async void When_include_has_no_conflicts()
        {
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            await Stream.WriteAsync(new Stream(partition), events);
            
            var actual = RetrieveTestEntity(entity.RowKey);
            Assert.That(actual, Is.Not.Null);
        }
        
        [Test]
        public async void When_include_has_conflict()
        {
            
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            events = new[] {CreateEvent("e3", include)};
            Assert.Throws<IncludedOperationConflictException>(
                async ()=> await Stream.WriteAsync(result.Stream, events));
        }

        [Test]
        public async void When_include_has_conflict_and_also_duplicate_event_conflict()
        {
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            events = new[] {CreateEvent("e1", include)};
            Assert.Throws<DuplicateEventException>(
                async () => await Stream.WriteAsync(result.Stream, events));
        }

        [Test]
        public async void When_include_has_conflict_and_also_stream_header_has_changed_since_last_read()
        {
            var entity = new TestEntity("INV-0001");
            var include = Include.Insert(entity);

            EventData[] events = {CreateEvent("e1"), CreateEvent("e2", include)};
            var result = await Stream.WriteAsync(new Stream(partition), events);

            partition.UpdateStreamEntity();

            entity = new TestEntity("INV-0001");
            include = Include.Insert(entity);

            events = new[] {CreateEvent("e3", include)};
            Assert.Throws<ConcurrencyConflictException>(
                async () => await Stream.WriteAsync(result.Stream, events));
        }

        TestEntity RetrieveTestEntity(string rowKey)
        {
            return table.CreateQuery<TestEntity>()
                        .Where(x =>
                               x.PartitionKey == partition.PartitionKey
                               && x.RowKey == rowKey)
                        .ToList()
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

            return new EventData(id, properties, includes);
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