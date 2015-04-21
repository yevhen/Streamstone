using System;
using System.Linq;

using Streamstone;

namespace Example.Scenarios
{
    public class S06_Include_additional_entities : Scenario
    {
        public override void Run()
        {
            var stream = new Stream(Partition);

            var events = new[]
            {
                new Event(id: "11"),
                new Event(id: "22")
            };

            var result = Stream.Write(Table, stream, events);

            Stream.Write(Table, result.Stream, events);
        }
    }
}