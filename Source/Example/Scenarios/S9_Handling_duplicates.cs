using System;
using System.Linq;

using Streamstone;

namespace Example.Scenarios
{
    public class S9_Handling_duplicates : Scenario
    {
        public override void Run()
        {
            var result = Stream.Write(Table, new Stream(Partition), new[]{new Event(id: "42")});

            try
            {
                var events = new[]
                {
                    new Event(id: "56"),
                    new Event(id: "42")  // conflicting (duplicate) event
                };

                Stream.Write(Table, result.Stream, events);
            }
            catch (DuplicateEventException e)
            {
                Console.WriteLine("Idempotency is based on ID of the event.");
                Console.WriteLine("An ID of conflicting event will be reported back as a property of DuplicateEventException.");
                Console.WriteLine("Here the conflicting event is: {0}", e.Id);
                Console.WriteLine("The caller can use this information to remove conflicting event from the batch and retry");
            }
        }
    }
}
