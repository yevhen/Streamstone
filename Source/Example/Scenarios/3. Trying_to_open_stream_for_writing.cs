using System;
using System.Linq;

using Streamstone;

namespace Example.Scenarios
{
    public class Trying_to_open_stream_for_writing : Scenario
    {
        public override void Run()
        {
            TryOpenNonExistentStream();            
            TryOpenExistentStream();            
        }

        void TryOpenNonExistentStream()
        {
            var existent = Stream.TryOpen(Table, Partition);

            Console.WriteLine("Trying to open non-existent stream. Found: {0}, Stream: {1}", 
                              existent.Found, existent.Stream == null ? "<null>" : "?!?");
        }

        void TryOpenExistentStream()
        {
            Stream.Provision(Table, Partition);

            var existent = Stream.TryOpen(Table, Partition);

            Console.WriteLine("Trying to open existent stream. Found: {0}, Stream: {1}\r\nEtag - {2}, Version - {3}",
                               existent.Found, existent.Stream, existent.Stream.ETag, existent.Stream.Version);
        }
    }
}
