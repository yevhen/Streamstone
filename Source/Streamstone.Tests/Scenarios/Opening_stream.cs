using System;
using System.Linq;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Opening_stream
    {
        const string partition = "test";
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = Storage.SetUp();
        }

        [Test]
        public async void When_stream_does_exists()
        {
            await Stream.ProvisionAsync(table, partition);
            Assert.NotNull(await Stream.OpenAsync(table, partition));
        }
        
        [Test]
        [ExpectedException(typeof(StreamNotFoundException))]
        public async void When_stream_does_not_exist()
        {
            await Stream.OpenAsync(table, partition);
        }

        [Test]
        public async void When_trying_to_open_and_stream_does_exists()
        {
            await Stream.ProvisionAsync(table, partition);
            
            var result = await Stream.TryOpenAsync(table, partition);
            
            Assert.That(result.Found, Is.True);
            Assert.That(result.Stream, Is.Not.Null);
        }
        
        [Test]
        public async void When_trying_to_open_and_stream_does_not_exist()
        {
            var result = await Stream.TryOpenAsync(table, partition);
            
            Assert.That(result.Found, Is.False);
            Assert.That(result.Stream, Is.Null);
        }
    }
}