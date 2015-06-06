using System;
using System.Collections.Generic;
using System.Linq;

using Streamstone;
using Microsoft.WindowsAzure.Storage.Table;

namespace Example.Scenarios
{
    public class S08_Concurrency_conflicts : Scenario
    {
        public override void Run()
        {
            SimultaneousProvisioning();
            SimultaneousWriting();
            SimultaneousSettingOfStreamMetadata();
            SequentiallyWritingToStreamIgnoringReturnedStreamHeader();
        }

        void SimultaneousProvisioning()
        {
            Stream.Provision(Partition);

            try
            {
                Stream.Provision(Partition);
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously provisioning stream in a same partition will lead to ConcurrencyConflictException");
            }
        }

        void SimultaneousWriting()
        {
            var a = Stream.Open(Partition);
            var b = Stream.Open(Partition);

            Stream.Write(a, new[]{new EventData("123")});
            
            try
            {
                Stream.Write(b, new[]{new EventData("456")});
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously writing to the same version of stream will lead to ConcurrencyConflictException");
            }
        }

        void SimultaneousSettingOfStreamMetadata()
        {
            var a = Stream.Open(Partition);
            var b = Stream.Open(Partition);

            Stream.SetProperties(a, StreamProperties.From(new {A = 42}));

            try
            {
                Stream.SetProperties(b, StreamProperties.From(new {A = 56}));
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Simultaneously setting metadata using the same version of stream will lead to ConcurrencyConflictException");
            }
        }

        void SequentiallyWritingToStreamIgnoringReturnedStreamHeader()
        {
            var stream = Stream.Open(Partition);

            var result = Stream.Write(stream, new[]{new EventData("AAA")});
            
            // a new stream header is returned after each write, it contains new Etag
            // and it should be used for subsequent operations
            // stream = result.Stream; 
            
            try
            {
                Stream.Write(stream, new[]{new EventData("BBB")});
            }
            catch (ConcurrencyConflictException)
            {
                Console.WriteLine("Ignoring new stream (header) returned after each Write() operation will lead to ConcurrencyConflictException on subsequent write operation");
            }
        }
    }
}
