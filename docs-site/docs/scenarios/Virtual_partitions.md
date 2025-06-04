# Virtual Partitions

This scenario demonstrates how to use virtual partitions in Streamstone.


```csharp title="Virtual_partitions.cs"
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Data.Tables;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Virtual_partitions
    {
        TableClient table;
        
        Partition partition;
        Partition virtual1;
        Partition virtual2;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();

            partition = new Partition(table, "test");
            virtual1  = new Partition(table, "test|123");
            virtual2  = new Partition(table, "test", "456");
        }

        [Test]
        public async Task When_provisioning()
        {
            await Stream.ProvisionAsync(virtual1);
            await Stream.ProvisionAsync(virtual2);

            Assert.That(partition.RetrieveAll().Count, Is.EqualTo(2));
        }

        [Test]
        public async Task When_opening()
        {
            await Stream.ProvisionAsync(virtual1);

            Assert.True((await Stream.TryOpenAsync(virtual1)).Found);
            Assert.False((await Stream.TryOpenAsync(virtual2)).Found);
        }

        [Test]
        public async Task When_writing_and_reading()
        {
            var stream1 = new Stream(virtual1);
            var stream2 = new Stream(virtual2);

            var e1 = CreateEvent("e1");
            var e2 = CreateEvent("e2");

            await Stream.WriteAsync(stream1, e1, e2);
            await Stream.WriteAsync(stream2, e1, e2);

            Assert.That(partition.RetrieveAll().Count, 
                Is.EqualTo(2 + 2*(2*2)));

            var slice1 = await Stream.ReadAsync<TestRecordedEventEntity>(virtual1);
            var slice2 = await Stream.ReadAsync<TestRecordedEventEntity>(virtual2);

            Assert.That(slice1.Events.Length, Is.EqualTo(2));
            Assert.That(slice2.Events.Length, Is.EqualTo(2));
        }

        static EventData CreateEvent(string id)
        {
            var properties = new Dictionary<string, object>
            {
                {"Type", "StreamChanged"},
                {"Data", "{}"}
            };

            return new EventData(EventId.From(id), EventProperties.From(properties));
        }
    }
}
``` 