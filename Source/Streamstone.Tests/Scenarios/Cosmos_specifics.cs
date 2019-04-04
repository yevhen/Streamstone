using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos.Table;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture, Explicit]
    public class Cosmos_specifics
    {
        CloudTable table;

        [SetUp]
        public void SetUp()
        {
            var connectionString = Environment.GetEnvironmentVariable(
                "Streamstone-Test-Cosmos-Storage", EnvironmentVariableTarget.User);

            var account = CloudStorageAccount.Parse(connectionString);
            var client = account.CreateCloudTableClient();

            table = client.GetTableReference("SSTEST");
            table.DeleteIfExistsAsync().GetAwaiter().GetResult();
            table.CreateIfNotExistsAsync().GetAwaiter().GetResult();
        }

        [Test]
        public async Task Check_can_write()
        {
            var partition = new Partition(table, "test-can-write");
            var stream = new Stream(partition);

            var events = Enumerable.Range(1, 10)
                .Select(CreateEvent)
                .ToArray();

            await Stream.WriteAsync(stream, events);
        }

        [Test]
        public async Task When_hitting_request_limit_during_writes()
        {
            const int streamsToWrite = 1000; // 800RU the default limit for CosmosDb table

            await Task.WhenAll(Enumerable.Range(1, streamsToWrite).Select(async streamIndex =>
            {
                var partition = new Partition(table, $"test-RU-limit-on-writes-stream-{streamIndex}");
                var stream = new Stream(partition);

                var events = Enumerable.Range(1, 10)
                    .Select(CreateEvent)
                    .ToArray();

                try
                {
                    await Stream.WriteAsync(stream, events);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    if (Debugger.IsAttached)
                        Environment.Exit(-1);
                }
            }));
        }

        static EventData CreateEvent(int num)
        {
            var properties = new Dictionary<string, EntityProperty>
            {
                {"Type", new EntityProperty("TestEvent")},
                {"Data", new EntityProperty(num)}
            };

            return new EventData(EventId.None, EventProperties.From(properties));
        }
    }
}