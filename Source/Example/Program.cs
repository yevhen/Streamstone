using System;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example
{
    using Scenarios;

    class Program
    {
        static async Task Main(string[] args)
        {
            var table = await Prepare();

            var scenarios = new Scenario[]
            {
                new S01_Provision_new_stream(),
                new S02_Open_stream_for_writing(),
                new S03_Try_open_stream_for_writing(),
                new S04_Write_to_stream(),
                new S05_Read_from_stream(),
                new S06_Include_additional_entities(),
                new S07_Custom_stream_metadata(),
                new S08_Concurrency_conflicts(),
                new S09_Handling_duplicates(),
                new S10_Stream_directory(),
                new S11_Sharding_streams(),
            };

            foreach (var scenario in scenarios)
            {
                var scenarioName = scenario.GetType().Name;

                Console.WriteLine("{0}", scenarioName.Replace("_", " "));
                Console.WriteLine(new string('-', 40));

                scenario.Initialize(table, scenarioName);
                await scenario.RunAsync();

                Console.WriteLine();
            }

            Console.WriteLine("You can check out the contents of '{0}' table using Server Explorer in VS", table.Name);
            Console.WriteLine("Press any key to exit ...");

            Console.ReadKey(true);
        }

        static async Task<CloudTable> Prepare()
        {
            var table = CloudStorageAccount
                .DevelopmentStorageAccount
                .CreateCloudTableClient()
                .GetTableReference("Example");

            await table.DeleteIfExistsAsync();
            await table.CreateIfNotExistsAsync();

            return table;
        }
    }
}
