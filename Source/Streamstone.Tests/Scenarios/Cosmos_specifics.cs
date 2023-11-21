using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

using Azure.Data.Tables;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture, Explicit]
    public class Cosmos_specifics
    {
        const string TableName = "SSTEST";
        TableClient table;

        [SetUp]
        public void SetUp()
        {
            var connectionString = Environment.GetEnvironmentVariable(
                "Streamstone-Test-Cosmos-Storage", EnvironmentVariableTarget.User);

            var client = new TableServiceClient(connectionString);

            table = client.GetTableClient(TableName);
            table.Delete();
            table.CreateIfNotExists();
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

        [Test]
        public void When_writing_duplicate_event()
        {
            var partition = new Partition(table, "test-cosmos-dupes");
            var stream = new Stream(partition);

            partition.InsertEventIdEntities("e1", "e2");
            partition.CaptureContents(contents =>
            {
                var duplicate = new EventData(EventId.From("e2"));

                Assert.ThrowsAsync<DuplicateEventException>(
                    async () => await Stream.WriteAsync(stream, new EventData(EventId.From("e3")), duplicate));

                contents.AssertNothingChanged();
            });
        }

        static EventData CreateEvent(int num)
        {
            var properties = new Dictionary<string, object>
            {
                {"Type", "TestEvent"},
                {"Data", num}
            };

            return new EventData(EventId.None, EventProperties.From(properties));
        }
    }
}