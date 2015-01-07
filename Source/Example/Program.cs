using System;
using System.Linq;

using Microsoft.WindowsAzure.Storage;

namespace Example
{
    using Scenarios;

    public static class Program
    {
        public static void Main()
        {
            var table = CloudStorageAccount
                .DevelopmentStorageAccount
                .CreateCloudTableClient()
                .GetTableReference("Example");
            
            table.DeleteIfExists();
            table.CreateIfNotExists();

            var scenarios = new Scenario[]
            {
                new Provisioning_new_empty_stream(),
                new Opening_stream_for_writing(),
                new Trying_to_open_stream_for_writing(),
                new Writing_to_stream(),
                new Reading_from_stream(),
                new Including_additional_table_operations(),
                new Using_custom_stream_metadata(),
                new Handling_concurrency_conflicts(),
                new Handling_duplicate_events()
            };

            for (int i = 0; i < scenarios.Length; i++)
            {
                var scenario = scenarios[i];

                Console.WriteLine("{0}", scenario.GetType().Name.Replace("_", " "));
                Console.WriteLine(new string('-', 40));

                scenario.Initialize(table, i);
                scenario.Run();

                Console.WriteLine();
            }

            Console.WriteLine("You can check out the contents of Example table using Server Explorer in VS");
            Console.WriteLine("Press any key to exit ...");

            Console.ReadKey(true);
        }
    }
}
