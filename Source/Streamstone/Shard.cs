namespace Streamstone
{
    /// <summary>
    /// Sharding support based on jump consistent hashing and XXHash algorithm.
    /// </summary>
    public static class Shard
    {
        /// <summary>
        /// Resolves shard index for a given key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="total">The total number of shards.</param>
        /// <returns>0-based shard index</returns>
        public static int Resolve(string key, int total)
        {
            Requires.GreaterThanOrEqualToOne(total, nameof(total));
            return Resolve(Hasher.Hash(key), total);
        }

        /// <summary>
        ///     Adapted from Go version found here https://github.com/renstrom/go-jump-consistent-hash/blob/master/jump.go
        /// </summary>
        /// <remarks>
        ///     For the full explanation of algorithm check this paper http://arxiv.org/abs/1406.2294. 
        ///     The plan english description is here https://godoc.org/github.com/renstrom/go-jump-consistent-hash
        /// </remarks>
        static int Resolve(ulong key, int total)
        {
            long b = 0, 
                 j = 0;

            while (j < total)
            {
                b = j;
                key = key * 2862933555777941757 + 1;
                j = (long)((b + 1) * (((long)1 << 31) / (double)((key >> 33) + 1)));
            }

            return (int)b;
        }
    }
}