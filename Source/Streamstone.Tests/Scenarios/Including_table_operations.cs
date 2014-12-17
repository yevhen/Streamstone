using System;
using System.Linq;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Including_table_operations
    {
        const string partition = "test";
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = StorageModel.SetUp();
        }

        [Test]
        public async void When_writing_number_of_events_plus_includes_is_over_max_batch_size_limit()
        {
            var stream = await Stream.ProvisionAsync(table, partition);

            var events = Enumerable
                .Range(1, (ApiModel.MaxEntitiesPerBatch / 2) + 1)
                .Select(i => CreateEvent("e" + i))
                .ToArray();

            var includes = Enumerable
                .Range(1, (ApiModel.MaxEntitiesPerBatch / 2) + 1)
                .Select(i => Include.Insert(new TestEntity()))
                .ToArray();

            table.CaptureContents(partition, contents =>
            {
                Assert.Throws<ArgumentOutOfRangeException>(
                    async () => await Stream.WriteAsync(table, stream, events, includes));

                contents.AssertNothingChanged();
            });
        }

        [Test]
        public async void When_operation_has_no_conflicts()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            
            var entity = new TestEntity(partition, "INV-0001");
            var include = Include.Insert(entity);

            var stream = await Stream.ProvisionAsync(table, partition);
            await Stream.WriteAsync(table, stream, events, new[]{include});
            
            var actual = RetrieveTestEntity(entity.RowKey);
            Assert.That(actual, Is.Not.Null);
        }
        
        [Test]
        public async void When_operation_has_conflict()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};
            
            var entity = new TestEntity(partition, "INV-0001");
            var include = Include.Insert(entity);

            var stream = await Stream.ProvisionAsync(table, partition);
            var result = await Stream.WriteAsync(table, stream, events, new[]{include});

            events = new[] {CreateEvent("e3")};
            entity = new TestEntity(partition, "INV-0001");
            include = Include.Insert(entity);

            Assert.Throws<IncludedOperationConflictException>(
                async ()=> await Stream.WriteAsync(table, result.Stream, events, new[] {include}));
        }

        [Test]
        public async void When_operation_has_conflict_and_also_duplicate_event_conflict()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};

            var entity = new TestEntity(partition, "INV-0001");
            var include = Include.Insert(entity);

            var stream = await Stream.ProvisionAsync(table, partition);
            var result = await Stream.WriteAsync(table, stream, events, new[] { include });

            events = new[] {CreateEvent("e1")};
            entity = new TestEntity(partition, "INV-0001");
            include = Include.Insert(entity);

            Assert.Throws<DuplicateEventException>(
                async () => await Stream.WriteAsync(table, result.Stream, events, new[] { include }));
        }

        [Test]
        public async void When_operation_has_conflict_and_also_stream_header_has_changed_since_last_read()
        {
            Event[] events = {CreateEvent("e1"), CreateEvent("e2")};

            var entity = new TestEntity(partition, "INV-0001");
            var include = Include.Insert(entity);

            var stream = await Stream.ProvisionAsync(table, partition);
            var result = await Stream.WriteAsync(table, stream, events, new[] { include });

            events = new[] {CreateEvent("e3")};
            entity = new TestEntity(partition, "INV-0001");
            include = Include.Insert(entity);

            table.UpdateStreamEntity(partition, count: 10);
            
            Assert.Throws<ConcurrencyConflictException>(
                async () => await Stream.WriteAsync(table, result.Stream, events, new[] { include }));
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
            var @event = new TestEventEntity
            {
                Id = id,
                Type = "StreamChanged",
                Data = "{}"
            };

            return new Event(@event.Id, EventProperties.From(@event));
        }

        class TestEntity : TableEntity
        {
            public TestEntity()
            {}

            public TestEntity(string partitionKey, string rowKey) 
                : base(partitionKey, rowKey)
            {
                Data = DateTime.UtcNow.ToString();
            }

            public string Data { get; set; }
        }
    }
}