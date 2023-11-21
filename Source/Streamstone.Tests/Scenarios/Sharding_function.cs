using System.Linq;

using NUnit.Framework;

namespace Streamstone.Scenarios
{
    [TestFixture]
    public class Sharding_function
    {
        [Test]
        public void When_resolving_shard()
        {
            const int shardsTotal = 10;
            const int keysTotal = 100000;

            var shards = Enumerable
                .Range(1, keysTotal)
                .Select(key => Shard.Resolve(key.ToString(), shardsTotal))
                .ToList();

            Assert.That(shards.Count, 
                Is.Not.EqualTo(shards.Distinct().Count()), 
                    "Should pick different shards");

            Assert.That(shards.Distinct().Count(), 
                Is.EqualTo(shardsTotal));

            var distribution = shards
                .GroupBy(shard => shard)
                .OrderBy(shard => shard.Key)
                .Select(shard => new {Id = shard.Key, Count = shard.Count()});

            const int ideal = keysTotal / shardsTotal;
            const int precision = (int)(ideal * 0.01); // within 1%
            const int lowBound  = ideal - precision;
            const int highBound = ideal + precision;

            foreach (var shard in distribution)
            {
                Assert.That(lowBound <= shard.Count && shard.Count <= highBound);
                // Console.WriteLine($"{shard.Id,-2}: {shard.Count}");
            }            
        }
    }
}