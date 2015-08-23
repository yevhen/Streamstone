using System;
using System.Linq;

using Streamstone;

namespace Example.Scenarios
{
    public class S09_Handling_duplicates : Scenario
    {
        public override void Run()
        {
            var result = Stream.Write(new Stream(Partition), new EventData(EventId.From("42")));

            try
            {
                var events = new[]
                {
                    new EventData(EventId.From("56")),
                    new EventData(EventId.From("42"))  // conflicting (duplicate) event
                };

                Stream.Write(result.Stream, events);
            }
            catch (DuplicateEventException e)
            {
                Console.WriteLine("Duplicate event detection is based on ID of the event.");
                Console.WriteLine("An ID of conflicting event will be reported back as a property of DuplicateEventException.");
                Console.WriteLine("Here the conflicting event is: {0}", e.Id);
                Console.WriteLine("The caller can use this information to remove conflicting event from the batch and retry (or cancel)");
            }
        }
    }
}
