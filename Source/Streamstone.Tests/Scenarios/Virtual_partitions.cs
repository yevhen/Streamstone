using System;
using System.Linq;
using System.Collections.Generic;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Virtual_partitions
    {
        const string partition  = "test"; 
        const string virtual1   = partition + "|123";
        const string virtual2   = partition + "|456"; 
        
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
        }

        [Test]
        public async void When_provisioning()
        {
            await Stream.ProvisionAsync(table, virtual1);
            await Stream.ProvisionAsync(table, virtual2);

            Assert.That(table.RetrieveAll(partition).Count, Is.EqualTo(2));
        }

        [Test]
        public async void When_opening()
        {
            await Stream.ProvisionAsync(table, virtual1);

            Assert.True(Stream.TryOpen(table, virtual1).Found);
            Assert.False(Stream.TryOpen(table, virtual2).Found);
        }

        [Test]
        public async void When_writing_and_reading()
        {
            var stream1 = new Stream(virtual1);
            var stream2 = new Stream(virtual2);

            var e1 = CreateEvent("e1");
            var e2 = CreateEvent("e2");

            await Stream.WriteAsync(table, stream1, new[] {e1, e2});
            await Stream.WriteAsync(table, stream2, new[] {e1, e2});

            Assert.That(table.RetrieveAll(partition).Count, 
                Is.EqualTo(2 + 2*(2*2)));

            var slice1 = await Stream.ReadAsync<TestRecordedEventEntity>(table, virtual1);
            var slice2 = await Stream.ReadAsync<TestRecordedEventEntity>(table, virtual2);

            Assert.That(slice1.Events.Length, Is.EqualTo(2));
            Assert.That(slice2.Events.Length, Is.EqualTo(2));
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