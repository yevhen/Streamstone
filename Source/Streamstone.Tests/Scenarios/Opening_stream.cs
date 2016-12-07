using System;
using System.Linq;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Opening_stream
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
        public async void When_stream_does_exists()
        {
            await Stream.ProvisionAsync(partition);
            Assert.NotNull(await Stream.OpenAsync(partition));
        }
        
        [Test]
        public void When_stream_does_not_exist()
        {
            Assert.ThrowsAsync<StreamNotFoundException>(async ()=> await Stream.OpenAsync(partition));
        }

        [Test]
        public async void When_trying_to_open_and_stream_does_exists()
        {
            await Stream.ProvisionAsync(partition);
            
            var result = await Stream.TryOpenAsync(partition);
            
            Assert.That(result.Found, Is.True);
            Assert.That(result.Stream, Is.Not.Null);
        }
        
        [Test]
        public async void When_trying_to_open_and_stream_does_not_exist()
        {
            var result = await Stream.TryOpenAsync(partition);
            
            Assert.That(result.Found, Is.False);
            Assert.That(result.Stream, Is.Null);
        }
    }
}