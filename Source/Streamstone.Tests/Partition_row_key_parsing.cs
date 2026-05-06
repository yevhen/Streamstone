using Azure.Data.Tables;
using NUnit.Framework;

namespace Streamstone
{
    [TestFixture]
    public class Partition_row_key_parsing
    {
        static TableClient CreateTable() =>
            new TableClient("UseDevelopmentStorage=true", "test");

        [Test]
        public void When_matching_row_keys_in_standard_partition()
        {
            var partition = new Partition(CreateTable(), "test");

            Assert.That(partition.IsStreamRowKey(partition.StreamRowKey()), Is.True);
            Assert.That(partition.IsEventVersionRowKey(partition.EventVersionRowKey(1)), Is.True);
            Assert.That(partition.IsEventIdRowKey(partition.EventIdRowKey("e1")), Is.True);
            Assert.That(partition.TryGetEventId(partition.EventIdRowKey("e1"), out var id), Is.True);
            Assert.That(id, Is.EqualTo("e1"));
        }

        [Test]
        public void When_matching_row_keys_in_virtual_partition()
        {
            var partition = new Partition(CreateTable(), "test|123");
            var other = new Partition(CreateTable(), "test|456");

            Assert.That(partition.IsStreamRowKey(partition.StreamRowKey()), Is.True);
            Assert.That(partition.IsStreamRowKey(other.StreamRowKey()), Is.False);

            Assert.That(partition.IsEventVersionRowKey(partition.EventVersionRowKey(1)), Is.True);
            Assert.That(partition.IsEventVersionRowKey(other.EventVersionRowKey(1)), Is.False);

            Assert.That(partition.IsEventIdRowKey(partition.EventIdRowKey("e1")), Is.True);
            Assert.That(partition.IsEventIdRowKey(other.EventIdRowKey("e1")), Is.False);

            Assert.That(partition.TryGetEventId(partition.EventIdRowKey("e1"), out var id), Is.True);
            Assert.That(id, Is.EqualTo("e1"));

            Assert.That(partition.TryGetEventId(other.EventIdRowKey("e1"), out _), Is.False);
        }
    }
}
