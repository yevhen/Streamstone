using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Reading_from_stream
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
        public void When_start_version_is_less_than_1()
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                async ()=> await Stream.ReadAsync<TestEventEntity>(partition, 0));

            Assert.Throws<ArgumentOutOfRangeException>(
                async ()=> await Stream.ReadAsync<TestEventEntity>(partition, -1));
        }
        
        [Test]
        public async void When_stream_is_empty()
        {
            await Stream.ProvisionAsync(partition);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition);
            
            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(0));
        }
        
        [Test]
        public async void When_version_is_greater_than_current_version_of_stream()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition, events.Length + 1);
            
            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(0));
        }

        [Test]
        public async void When_all_events_fit_to_single_slice()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestEventEntity>(partition, sliceSize: 2);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(2));
        }

        [Test]
        public async void When_all_events_do_not_fit_single_slice()
        {
            EventData[] events = {CreateEvent("e1"), CreateEvent("e2")};
            await Stream.WriteAsync(new Stream(partition), events);

            var slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, sliceSize: 1);
            
            Assert.That(slice.IsEndOfStream, Is.False);
            Assert.That(slice.Events.Length, Is.EqualTo(1));
            Assert.That(slice.Events[0].Version, Is.EqualTo(1));

            slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, slice.NextEventNumber);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(1));
            Assert.That(slice.Events[0].Version, Is.EqualTo(2));
        }

        [Test, Explicit]
        public async void When_slice_size_is_bigger_than_azure_storage_page_limit()
        {
            const int sizeOverTheAzureLimit = 1500;
            const int numberOfWriteBatches = 50;

            var stream = await Stream.ProvisionAsync(partition);

            foreach (var batch in Enumerable.Range(1, numberOfWriteBatches))
            {
                EventData[] events = Enumerable
                    .Range(1, sizeOverTheAzureLimit / numberOfWriteBatches)
                    .Select(i => CreateEvent(batch + "e" + i))
                    .ToArray();

                var result = await Stream.WriteAsync(stream, events);
                stream = result.Stream;
            }

            var slice = await Stream.ReadAsync<TestRecordedEventEntity>(partition, sliceSize: 1500);

            Assert.That(slice.IsEndOfStream, Is.True);
            Assert.That(slice.Events.Length, Is.EqualTo(1500));
        }

        static EventData CreateEvent(string id)
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Id",   new EntityProperty(id)},
                {"Type", new EntityProperty("StreamChanged")},
                {"Data", new EntityProperty("{}")}
            };

            return new EventData(id, EventProperties.From(properties));
        }
    }
}