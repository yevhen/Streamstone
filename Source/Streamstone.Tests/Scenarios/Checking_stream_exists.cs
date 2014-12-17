using System;
using System.Linq;

using NUnit.Framework;
using Microsoft.WindowsAzure.Storage.Table;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Checking_stream_exists
    {
        const string partition = "test";
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            table = StorageModel.SetUp();
        }

        [Test]
        public async void When_stream_does_exists()
        {
            await Stream.ProvisionAsync(table, partition);
            Assert.True(await Stream.ExistsAsync(table, partition));
        }
        
        [Test]
        public async void When_stream_does_not_exist()
        {
            Assert.False(await Stream.ExistsAsync(table, partition));
        }
    }
}